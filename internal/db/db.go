package db

import "github.com/santhoshm25/key-value-ds/types"

type Database interface {
	CreateUser(user *types.User) error
	GetUser(userName string) (*types.User, error)
	CreateObject(userID int, obj *types.Object) error
	GetObject(userID int, key string) (*types.Object, error)
	DeleteObject(userID int, key string) error
	BatchCreateObject(userID int, objs []*types.Object) error
}
