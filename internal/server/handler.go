package server

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/santhoshm25/key-value-ds/internal/auth"
	"github.com/santhoshm25/key-value-ds/internal/db"
	"github.com/santhoshm25/key-value-ds/types"
	"github.com/santhoshm25/key-value-ds/utils"

	"github.com/julienschmidt/httprouter"
)

const (
	minTTL     = 0
	maxTTL     = 4102444800
	maxKeySize = 32
)

func RegisterHandler(db db.Database) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		user := &types.User{}

		err := utils.ExtractRequestBody(r.Body, user)
		if err != nil {
			err = utils.ErrBadRequest(utils.InvalidBodyErr)
			sendHTTPResponse(nil, err, w)
			return
		}

		err = auth.Register(db, user)
		sendHTTPResponse(nil, err, w)
	}
}

func LoginHandler(db db.Database) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		user := &types.User{}

		err := utils.ExtractRequestBody(r.Body, user)
		if err != nil {
			err = utils.ErrBadRequest(utils.InvalidBodyErr)
			sendHTTPResponse(nil, err, w)
			return
		}

		body, err := auth.Login(db, user)
		sendHTTPResponse(body, err, w)
	}
}

func AuthHandler(db db.Database, h httprouter.Handle) func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		token := r.Header.Get("Authorization")
		if token == "" {
			sendHTTPResponse(nil, utils.ErrUnAuthorized("authorization token not found"), w)
			return
		}

		claims, err := auth.ValidateToken(token)
		if err != nil {
			sendHTTPResponse(nil, utils.ErrUnAuthorized(err.Error()), w)
			return
		}
		ps = append(ps, httprouter.Param{Key: "user_id", Value: fmt.Sprintf("%d", claims.UserID)})

		h(w, r, ps)
	}
}

func CreateObjectHandler(db db.Database) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		userId, err := extractUserId(ps)
		if err != nil {
			sendHTTPResponse(nil, err, w)
			return
		}

		object := &types.Object{}
		err = utils.ExtractRequestBody(r.Body, object)
		if err != nil {
			sendHTTPResponse(nil, err, w)
			return
		}

		if err := validateObject(object); err != nil {
			sendHTTPResponse(nil, err, w)
			return
		}

		err = db.CreateObject(userId, object)
		sendHTTPResponse(nil, err, w)
	}
}

func GetObjectHandler(db db.Database) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		userID, err := extractUserId(ps)
		if err != nil {
			sendHTTPResponse(nil, err, w)
			return
		}
		key := ps.ByName("key")
		object, err := db.GetObject(userID, key)
		sendHTTPResponse(object, err, w)
	}
}

func DeleteObjectHandler(db db.Database) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		userID, err := extractUserId(ps)
		if err != nil {
			sendHTTPResponse(nil, err, w)
			return
		}
		key := ps.ByName("key")
		err = db.DeleteObject(userID, key)
		sendHTTPResponse(nil, err, w)
	}
}

func BatchCreateObjectHandler(db db.Database) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		userId, err := extractUserId(ps)
		if err != nil {
			sendHTTPResponse(nil, err, w)
			return
		}

		var objects []*types.Object
		err = utils.ExtractRequestBody(r.Body, &objects)
		if err != nil {
			sendHTTPResponse(nil, err, w)
			return
		}

		err = db.BatchCreateObject(userId, objects)
		sendHTTPResponse(nil, err, w)
	}
}

func sendHTTPResponse(body any, err error, w http.ResponseWriter) {
	var statusCode int
	var resp any

	if err == nil {
		if body == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		statusCode = http.StatusOK
		resp = body
	} else {
		if respErr, ok := err.(*utils.Error); ok {
			statusCode = respErr.Code
			resp = respErr
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	writeResponse(statusCode, resp, w)
}

func writeResponse(statusCode int, resp any, w http.ResponseWriter) {
	respBody, marshalErr := utils.MarshalResponse(resp)
	if marshalErr != nil {
		http.Error(w, marshalErr.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	w.Write(respBody)
}

func extractUserId(ps httprouter.Params) (int, error) {
	userID := ps.ByName("user_id")
	userIDInt, err := strconv.Atoi(userID)
	if err != nil {
		return 0, utils.ErrBadRequest("invalid user ID")
	}
	return userIDInt, nil
}

func validateObject(obj *types.Object) error {
	if obj.TTL < minTTL || obj.TTL > maxTTL {
		return utils.ErrBadRequest("ttl must be between 0 and 31536000")
	}
	if len(obj.Key) > maxKeySize {
		return utils.ErrBadRequest("key size exceeded, must be within %d characters", maxKeySize)
	}
	return nil
}
