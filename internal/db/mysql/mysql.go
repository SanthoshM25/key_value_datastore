package mysql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/santhoshm25/key-value-ds/internal/server"
	"github.com/santhoshm25/key-value-ds/types"
	"github.com/santhoshm25/key-value-ds/utils"
)

const (
	maxValueSize = 16384 //16KB
)

type MysqlDB struct {
	Db *sql.DB
}

func NewDB() *MysqlDB {
	return &MysqlDB{}
}

func (msDB *MysqlDB) Init() {
	dsn := os.Getenv("DATABASE_URL")

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		slog.Error("error opening database", "error", err)
		os.Exit(1)
	}

	err = db.Ping()
	if err != nil {
		slog.Error("error pinging database", "error", err)
		os.Exit(1)
	}

	msDB.Db = db
	slog.Info("Successfully connected to the database!")
}

func (msDB *MysqlDB) CreateUser(user *types.User) (err error) {
	return msDB.withTransaction("user creation", utils.UserCreateErr, utils.ErrStatusCreated(utils.UserCreated), func(tx *sql.Tx) error {
		var id int64
		{
			res, err := tx.Exec("INSERT INTO users (name, password) VALUES (?, ?)", user.Name, user.Password)
			if err != nil {
				if mysqlErr, ok := err.(*mysql.MySQLError); ok && mysqlErr.Number == 1062 {
					return utils.ErrBadRequest(utils.UserExistsErr)
				}
				slog.Error("error creating user", "error", err)
				return utils.ErrInternalServer(utils.UserCreateErr)
			}
			id, _ = res.LastInsertId()
			slog.Info("user created", "id", id)
		}
		{
			_, err = tx.Exec("INSERT INTO quotas (user_id, provisioned, utilised) VALUES (?, ?, ?)", id, user.ProvisionedCapacity, 0)
			if err != nil {
				slog.Error("error creating user", "error", err)
				return utils.ErrInternalServer(utils.UserCreateErr)
			}
		}
		return nil
	})
}

func (msDB *MysqlDB) GetUser(userName string) (*types.User, error) {
	user := &types.User{}

	err := msDB.Db.QueryRow("SELECT id, password FROM users WHERE name = ?", userName).Scan(&user.ID, &user.Password)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, utils.ErrNotFound(utils.UserNotFoundErr)
		}
		slog.Error("error getting user", "error", err)
		return nil, utils.ErrInternalServer(utils.UserGetErr)
	}
	return user, nil
}

func (msDB *MysqlDB) CreateObject(userID int, obj *types.Object) error {
	return msDB.withTransaction("object creation", utils.ObjectCreateErr, utils.ErrStatusCreated(utils.ObjectCreated), func(tx *sql.Tx) error {
		quota := &types.Quota{}
		var valBytes []byte
		{
			err := tx.QueryRow("SELECT provisioned, utilised FROM quotas WHERE user_id = ?", userID).Scan(&quota.Provisioned, &quota.Utilised)
			if err != nil {
				slog.Error("error getting quota", "error", err)
				return utils.ErrInternalServer(utils.ObjectCreateErr)
			}
			valBytes, err = json.Marshal(obj.Value)
			if err != nil {
				slog.Error("error marshalling value", "error", err)
				return utils.ErrInternalServer(utils.ObjectCreateErr)
			}
			if err = validateQuota(quota, valBytes); err != nil {
				slog.Error("error validating object", "error", err.Error())
				return err
			}
		}
		{
			_, err := tx.Exec("REPLACE INTO data_store (user_id, data_key, data_value, ttl) VALUES (?, ?, ?, ?)", userID, obj.Key, valBytes, obj.TTL)
			if err != nil {
				slog.Error("error creating object", "error", err)
				return utils.ErrInternalServer(utils.ObjectCreateErr)
			}
		}
		{
			_, err := tx.Exec("UPDATE quotas SET utilised = utilised + ? WHERE user_id = ?", len(valBytes), userID)
			if err != nil {
				slog.Error("error updating quota", "error", err)
				return utils.ErrInternalServer(utils.ObjectCreateErr)
			}
		}
		return nil
	})
}

func validateQuota(quota *types.Quota, value []byte) error {
	if (quota.Utilised + int64(len(value))) > quota.Provisioned {
		return utils.ErrForbidden(utils.QuotaExceededErr)
	}
	if len(value) > maxValueSize {
		return utils.ErrBadRequest("value size exceeded, must be within %d bytes", maxValueSize)
	}
	return nil
}

func (msDB *MysqlDB) GetObject(userID int, key string) (*types.Object, error) {
	var valBytes []byte
	obj := &types.Object{Key: key}

	err := msDB.Db.QueryRow("SELECT data_value, ttl FROM data_store WHERE user_id = ? AND data_key = ?", userID, key).Scan(&valBytes, &obj.TTL)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, utils.ErrNotFound(utils.ObjectNotFoundErr)
		}
		slog.Error("error getting object", "error", err)
		return nil, utils.ErrInternalServer(utils.ObjectGetErr)
	}
	err = json.Unmarshal(valBytes, &obj.Value)
	if err != nil {
		slog.Error("error unmarshalling value", "error", err)
		return nil, utils.ErrInternalServer(utils.ObjectGetErr)
	}
	return obj, nil
}

func (msDB *MysqlDB) DeleteObject(userID int, key string) error {
	return msDB.withTransaction("object deletion", utils.ObjectDeleteErr, nil, func(tx *sql.Tx) error {
		var valBytes []byte
		{
			err := tx.QueryRow("SELECT data_value FROM data_store WHERE user_id = ? AND data_key = ?", userID, key).
				Scan(&valBytes)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return nil
				}
				slog.Error("error deleting object", "error", err)
				return utils.ErrInternalServer(utils.ObjectDeleteErr)
			}
		}
		{
			_, err := tx.Exec("DELETE FROM data_store WHERE user_id = ? AND data_key = ?", userID, key)
			if err != nil {
				slog.Error("error deleting object", "error", err)
				return utils.ErrInternalServer(utils.ObjectDeleteErr)
			}
		}
		{
			_, err := tx.Exec("UPDATE quotas SET utilised = utilised - ? WHERE user_id = ?", len(valBytes), userID)
			if err != nil {
				slog.Error("error updating quota", "error", err)
				return utils.ErrInternalServer(utils.ObjectDeleteErr)
			}
		}
		return nil
	})
}

func (msDB *MysqlDB) BatchCreateObject(userID int, objs []*types.Object) (err error) {
	return msDB.withTransaction("batch object creation", utils.ObjectBatchCreateErr, utils.ErrStatusCreated(utils.ObjectCreated), func(tx *sql.Tx) error {
		quota := &types.Quota{}

		err = tx.QueryRow("SELECT provisioned, utilised FROM quotas WHERE user_id = ?", userID).Scan(&quota.Provisioned, &quota.Utilised)
		if err != nil {
			slog.Error("error getting quota", "error", err)
			return utils.ErrInternalServer(utils.ObjectBatchCreateErr)
		}

		batchSize, queryPlaceholders, queryArgs, err := server.ValidateAndPrepareBatchRequest(userID, objs, quota.Provisioned-quota.Utilised)
		if err != nil {
			slog.Error("error validating and preparing batch request", "error", err.Error())
			return err
		}

		query := fmt.Sprintf("REPLACE INTO data_store (user_id, data_key, data_value, ttl) VALUES %s", strings.Join(queryPlaceholders, ","))
		_, err = tx.Exec(query, queryArgs...)
		if err != nil {
			slog.Error("error executing batch create object", "error", err)
			return utils.ErrInternalServer(utils.ObjectBatchCreateErr)
		}

		_, err = tx.Exec("UPDATE quotas SET utilised = utilised + ? WHERE user_id = ?", batchSize, userID)
		if err != nil {
			slog.Error("error updating quota", "error", err)
			return utils.ErrInternalServer(utils.ObjectBatchCreateErr)
		}
		return nil
	})
}

func (msDB *MysqlDB) withTransaction(operation, errorMsg string, successCode error, fn func(*sql.Tx) error) error {
	tx, err := msDB.Db.Begin()
	if err != nil {
		slog.Error("error beginning transaction for "+operation, "error", err)
		return utils.ErrInternalServer(errorMsg)
	}

	var opErr error
	defer func() {
		err = handleTxResult(tx, opErr)
	}()

	opErr = fn(tx)
	if opErr != nil {
		return opErr
	}

	return successCode
}

func handleTxResult(tx *sql.Tx, err error) error {
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			slog.Error("error rolling back transaction", "error", rollbackErr)
			err = utils.ErrInternalServer("")
		}
		return err
	}

	if commitErr := tx.Commit(); commitErr != nil {
		slog.Error("error committing transaction", "error", commitErr)
		return utils.ErrInternalServer("")
	}

	return nil
}
