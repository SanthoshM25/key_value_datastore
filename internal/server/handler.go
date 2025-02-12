package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/santhoshm25/key-value-ds/internal/auth"
	"github.com/santhoshm25/key-value-ds/internal/db"
	"github.com/santhoshm25/key-value-ds/types"
	"github.com/santhoshm25/key-value-ds/utils"

	"github.com/julienschmidt/httprouter"
)

func RegisterHandler(db db.Database) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		user := &types.User{}
		err := utils.ExtractRequestBody(r.Body, user)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := auth.Register(db, user); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("User registered successfully"))
	}
}

func LoginHandler(db db.Database) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		user := &types.User{}
		err := utils.ExtractRequestBody(r.Body, user)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		token, err := auth.Login(db, user)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte(token))
	}
}

func AuthHandler(db db.Database, h httprouter.Handle) func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		token := r.Header.Get("Authorization")
		if token == "" {
			http.Error(w, "Authorization token not found", http.StatusUnauthorized)
			return
		}
		claims, err := auth.ValidateToken(token)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		ps = append(ps, httprouter.Param{Key: "user_id", Value: fmt.Sprintf("%d", claims.UserID)})
		h(w, r, ps)
	}
}

func CreateObjectHandler(db db.Database) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		userID := ps.ByName("user_id")
		userIDInt, err := strconv.Atoi(userID)
		if err != nil {
			http.Error(w, "Invalid user id", http.StatusBadRequest)
		}
		object := &types.Object{}
		err = utils.ExtractRequestBody(r.Body, object)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Println("object", object)
		if err := db.CreateObject(userIDInt, object); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("Object created successfully"))
	}
}

func GetObjectHandler(db db.Database) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		userID := ps.ByName("user_id")
		userIDInt, err := strconv.Atoi(userID)
		if err != nil {
			http.Error(w, "Invalid user id", http.StatusBadRequest)
		}
		key := ps.ByName("key")
		object, err := db.GetObject(userIDInt, key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp, err := json.Marshal(object)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(resp)
	}
}

func DeleteObjectHandler(db db.Database) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		//TODO: put this user_id handling into an function
		userID := ps.ByName("user_id")
		userIDInt, err := strconv.Atoi(userID)
		if err != nil {
			http.Error(w, "Invalid user id", http.StatusBadRequest)
		}
		key := ps.ByName("key")
		if err := db.DeleteObject(userIDInt, key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("Object deleted successfully"))
	}
}

func BatchCreateObjectHandler(db db.Database) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		userID := ps.ByName("user_id")
		userIDInt, err := strconv.Atoi(userID)
		if err != nil {
			http.Error(w, "Invalid user id", http.StatusBadRequest)
		}
		var objects []*types.Object
		err = utils.ExtractRequestBody(r.Body, &objects)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := db.BatchCreateObject(userIDInt, objects); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("Objects created successfully using batch operation"))
	}
}
