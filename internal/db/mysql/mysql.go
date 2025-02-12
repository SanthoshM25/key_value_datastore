package mysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"

	"github.com/santhoshm25/key-value-ds/types"
)

type MysqlDB struct {
	Db *sql.DB
}

func NewDB() *MysqlDB {
	return &MysqlDB{}
}

func (msDB *MysqlDB) Init() {
	//TODO: use env
	dsn := ""

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	msDB.Db = db
	fmt.Println("Successfully connected to the database!")
}

func (msDB *MysqlDB) CreateUser(user *types.User) error {
	//TODO: implement as an transaction
	res, err := msDB.Db.Exec("INSERT INTO users (name, password) VALUES (?, ?)", user.Name, user.Password)
	//TODO: handle duplicate user name entry
	if err != nil {
		if strings.Contains(err.Error(), "Duplicate entry") {
			return fmt.Errorf("user already exists")
		}
		return fmt.Errorf("error creating user: %s", err.Error())
	}
	id, _ := res.LastInsertId()
	_, err = msDB.Db.Exec("INSERT INTO quotas (user_id, provisioned, utilised) VALUES (?, ?, ?)", id, user.ProvisionedCapacity, 0)
	if err != nil {
		return fmt.Errorf("error creating user quota: %s", err.Error())
	}
	return nil
}

func (msDB *MysqlDB) GetUser(userName string) (*types.User, error) {
	user := &types.User{}
	err := msDB.Db.QueryRow("SELECT id, password FROM users WHERE name = ?", userName).Scan(&user.ID, &user.Password)
	if err != nil {
		return nil, fmt.Errorf("error fetching user: %s", err.Error())
	}
	return user, nil
}

func (msDB *MysqlDB) CreateObject(userID int, obj *types.Object) error {
	// TODO validate size of key and value
	quota := &types.Quota{}
	err := msDB.Db.QueryRow("SELECT provisioned, utilised FROM quotas WHERE user_id = ?", userID).Scan(&quota.Provisioned, &quota.Utilised)
	if err != nil {
		return fmt.Errorf("error fetching user quota: %s", err.Error())
	}
	valbytes, err := json.Marshal(obj.Value)
	if err != nil {
		return fmt.Errorf("error marshalling value: %s", err.Error())
	}
	if err := validateQuota(quota, valbytes); err != nil {
		return err
	}
	_, err = msDB.Db.Exec("REPLACE INTO data_store (user_id, data_key, data_value, ttl) VALUES (?, ?, ?, ?)", userID, obj.Key, valbytes, obj.TTL)
	if err != nil {
		return fmt.Errorf("error inserting object: %s", err.Error())
	}
	_, err = msDB.Db.Exec("UPDATE quotas SET utilised = utilised + ? WHERE user_id = ?", len(valbytes), userID)
	if err != nil {
		return fmt.Errorf("error updating user quota: %s", err.Error())
	}
	return nil
}

func validateQuota(quota *types.Quota, value []byte) error {
	if (quota.Utilised + int64(len(value))) > quota.Provisioned {
		return fmt.Errorf("quota exceeded")
	}
	return nil
}

func (msDB *MysqlDB) GetObject(userID int, key string) (*types.Object, error) {
	obj := &types.Object{Key: key}
	var valBytes []byte
	err := msDB.Db.QueryRow("SELECT data_value, ttl FROM data_store WHERE user_id = ? AND data_key = ?", userID, key).
		Scan(&valBytes, &obj.TTL)
	if err != nil {
		return nil, fmt.Errorf("error fetching object: %s", err.Error())
	}
	err = json.Unmarshal(valBytes, &obj.Value)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling value: %s", err.Error())
	}
	return obj, nil
}

func (msDB *MysqlDB) DeleteObject(userID int, key string) error {
	var valBytes []byte
	err := msDB.Db.QueryRow("SELECT data_value FROM data_store WHERE user_id = ? AND data_key = ?", userID, key).
		Scan(&valBytes)
	if err != nil {
		return fmt.Errorf("error fetching object: %s", err.Error())
	}
	_, err = msDB.Db.Exec("DELETE FROM data_store WHERE user_id = ? AND data_key = ?", userID, key)
	if err != nil {
		return fmt.Errorf("error deleting object: %s", err.Error())
	}
	// valBytes = bytes.ReplaceAll(valBytes, []byte(" "), []byte(""))
	_, err = msDB.Db.Exec("UPDATE quotas SET utilised = utilised - ? WHERE user_id = ?", len(valBytes), userID)
	if err != nil {
		return fmt.Errorf("error updating user quota: %s", err.Error())
	}
	return nil
}

func (msDB *MysqlDB) BatchCreateObject(userID int, objs []*types.Object) error {
	quota := &types.Quota{}
	err := msDB.Db.QueryRow("SELECT provisioned, utilised FROM quotas WHERE user_id = ?", userID).Scan(&quota.Provisioned, &quota.Utilised)
	if err != nil {
		return fmt.Errorf("error fetching user quota: %s", err.Error())
	}
	batchSize, queryPlaceholders, queryArgs, err := validateAndPrepareBatchRequest(userID, objs, quota.Provisioned-quota.Utilised)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("REPLACE INTO data_store (user_id, data_key, data_value, ttl) VALUES %s", strings.Join(queryPlaceholders, ","))
	fmt.Println("query", query)
	_, err = msDB.Db.Exec(query, queryArgs...)
	if err != nil {
		return fmt.Errorf("error during batch insertion: %s", err.Error())
	}

	_, err = msDB.Db.Exec("UPDATE quotas SET utilised = utilised + ? WHERE user_id = ?", batchSize, userID)
	if err != nil {
		return fmt.Errorf("error updating user quota: %s", err.Error())
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
			return 0, []string{}, []any{}, fmt.Errorf("error marshalling value: %s", err.Error())
		}
		obj.Value = valBytes
		batchSize += int64(len(valBytes))
		queryPlaceholders[idx] = "(?, ?, ?, ?)"
		queryArgs = append(queryArgs, userID, obj.Key, obj.Value, obj.TTL)
	}
	if batchSize > availableBytes {
		return 0, []string{}, []any{}, fmt.Errorf("quota exceeded")
	}
	if batchSize > types.MaxBatchLimit {
		return 0, []string{}, []any{}, fmt.Errorf("batch size limit exceeded, max limit is %d", types.MaxBatchLimit)
	}
	return batchSize, queryPlaceholders, queryArgs, nil
}
