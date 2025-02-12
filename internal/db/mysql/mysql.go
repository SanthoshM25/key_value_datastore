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

	"github.com/santhoshm25/key-value-ds/types"
	"github.com/santhoshm25/key-value-ds/utils"
)

const (
	maxBatchLimit = 4194304 //4MB
	maxValueSize  = 16384   //16KB
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
	var id int64
	tx, err := msDB.Db.Begin()
	if err != nil {
		slog.Error("error beginning transaction while user creation", "error", err)
		return utils.ErrInternalServer(utils.UserCreateErr)
	}

	defer func() {
		err = handleTx(tx, err)
		if err == nil {
			err = utils.ErrStatusCreated(utils.UserCreated)
		}
	}()

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

func (msDB *MysqlDB) CreateObject(userID int, obj *types.Object) (err error) {
	quota := &types.Quota{}
	var valBytes []byte

	tx, err := msDB.Db.Begin()
	if err != nil {
		slog.Error("error beginning transaction while object creation", "error", err)
		return utils.ErrInternalServer(utils.ObjectCreateErr)
	}

	defer func() {
		err = handleTx(tx, err)
		if err == nil {
			err = utils.ErrStatusCreated(utils.ObjectCreated)
		}
	}()

	{
		err = tx.QueryRow("SELECT provisioned, utilised FROM quotas WHERE user_id = ?", userID).Scan(&quota.Provisioned, &quota.Utilised)
		if err != nil {
			slog.Error("error getting quota", "error", err)
			return utils.ErrInternalServer(utils.ObjectCreateErr)
		}
		valBytes, err = json.Marshal(obj.Value)
		if err != nil {
			slog.Error("error marshalling value", "error", err)
			return utils.ErrInternalServer(utils.ObjectCreateErr)
		}
		if err := validateQuota(quota, valBytes); err != nil {
			slog.Error("error validating object", "error", err)
			return utils.ErrBadRequest(err.Error())
		}
	}
	{
		_, err = tx.Exec("REPLACE INTO data_store (user_id, data_key, data_value, ttl) VALUES (?, ?, ?, ?)", userID, obj.Key, valBytes, obj.TTL)
		if err != nil {
			slog.Error("error creating object", "error", err)
			return utils.ErrInternalServer(utils.ObjectCreateErr)
		}
	}
	{
		_, err = tx.Exec("UPDATE quotas SET utilised = utilised + ? WHERE user_id = ?", len(valBytes), userID)
		if err != nil {
			slog.Error("error updating quota", "error", err)
			return utils.ErrInternalServer(utils.ObjectCreateErr)
		}
	}
	return nil
}

func validateQuota(quota *types.Quota, value []byte) error {
	if (quota.Utilised + int64(len(value))) > quota.Provisioned {
		return fmt.Errorf(utils.QuotaExceededErr)
	}
	if len(value) > maxValueSize {
		return fmt.Errorf("value size exceeded, must be within %d bytes", maxValueSize)
	}
	return nil
}

func (msDB *MysqlDB) GetObject(userID int, key string) (*types.Object, error) {
	var valBytes []byte
	obj := &types.Object{Key: key}

	err := msDB.Db.QueryRow("SELECT data_value, ttl FROM data_store WHERE user_id = ? AND data_key = ?", userID, key).Scan(&valBytes, &obj.TTL)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, utils.ErrNotFound("")
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

func (msDB *MysqlDB) DeleteObject(userID int, key string) (err error) {
	var valBytes []byte

	tx, err := msDB.Db.Begin()
	if err != nil {
		slog.Error("error beginning transaction while object deletion", "error", err)
		return utils.ErrInternalServer(utils.ObjectDeleteErr)
	}

	defer func() {
		err = handleTx(tx, err)
	}()

	{
		err = tx.QueryRow("SELECT data_value FROM data_store WHERE user_id = ? AND data_key = ?", userID, key).
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
		_, err = tx.Exec("DELETE FROM data_store WHERE user_id = ? AND data_key = ?", userID, key)
		if err != nil {
			slog.Error("error deleting object", "error", err)
			return utils.ErrInternalServer(utils.ObjectDeleteErr)
		}
	}
	{
		_, err = tx.Exec("UPDATE quotas SET utilised = utilised - ? WHERE user_id = ?", len(valBytes), userID)
		if err != nil {
			slog.Error("error updating quota", "error", err)
			return utils.ErrInternalServer(utils.ObjectDeleteErr)
		}
	}
	return nil
}

func (msDB *MysqlDB) BatchCreateObject(userID int, objs []*types.Object) (err error) {
	quota := &types.Quota{}

	tx, err := msDB.Db.Begin()
	if err != nil {
		slog.Error("error beginning transaction while batch object creation", "error", err)
		return utils.ErrInternalServer(utils.ObjectBatchCreateErr)
	}

	defer func() {
		err = handleTx(tx, err)
		if err == nil {
			err = utils.ErrStatusCreated(utils.ObjectCreated)
		}
	}()

	err = tx.QueryRow("SELECT provisioned, utilised FROM quotas WHERE user_id = ?", userID).Scan(&quota.Provisioned, &quota.Utilised)
	if err != nil {
		slog.Error("error getting quota", "error", err)
		return utils.ErrInternalServer(utils.ObjectBatchCreateErr)
	}

	batchSize, queryPlaceholders, queryArgs, err := validateAndPrepareBatchRequest(userID, objs, quota.Provisioned-quota.Utilised)
	if err != nil {
		slog.Error("error validating and preparing batch request", "error", err)
		return utils.ErrBadRequest(err.Error())
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
}

func validateAndPrepareBatchRequest(userID int, objs []*types.Object, availableBytes int64) (int64, []string, []any, error) {
	batchSize := int64(0)
	queryPlaceholders := make([]string, len(objs))
	queryArgs := make([]any, 0)

	for idx, obj := range objs {
		valBytes, err := json.Marshal(obj.Value)
		if err != nil {
			slog.Error("error marshalling value", "error", err)
			return 0, []string{}, []any{}, fmt.Errorf("error marshalling value: %s", err.Error())
		}

		obj.Value = valBytes
		batchSize += int64(len(valBytes))
		slog.Info("batchSize", "value", batchSize)
		queryPlaceholders[idx] = "(?, ?, ?, ?)"
		queryArgs = append(queryArgs, userID, obj.Key, obj.Value, obj.TTL)
	}
	if batchSize > availableBytes {
		return 0, []string{}, []any{}, fmt.Errorf(utils.QuotaExceededErr)
	}
	if batchSize > maxBatchLimit {
		return 0, []string{}, []any{}, fmt.Errorf("batch size limit exceeded, max limit is %d", maxBatchLimit)
	}
	return batchSize, queryPlaceholders, queryArgs, nil
}

func handleTx(tx *sql.Tx, err error) error {
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
