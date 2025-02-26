package utils

import (
	"fmt"
	"net/http"
)

const (
	UserExistsErr        = "user already exists"
	UserNotFoundErr      = "user not found"
	UserCreateErr        = "error creating user"
	UserGetErr           = "error getting user"
	UserCreated          = "user registered successfully"
	ObjectCreateErr      = "error creating object"
	ObjectGetErr         = "error getting object"
	ObjectDeleteErr      = "error deleting object"
	ObjectBatchCreateErr = "error creating objects"
	ObjectCreated        = "object created successfully"
	ObjectNotFoundErr    = "object not found"
	QuotaExceededErr     = "quota exceeded"
	InvalidBodyErr       = "invalid request body"
	EmptyBodyErr         = "empty request body"
	InvalidCredErr       = "invalid username/password"
)

type Error struct {
	Message string `json:"message"`
	Code    int    `json:"status_code"`
}

func (e Error) Error() string {
	return e.Message
}

func (e Error) StatusCode() int {
	return e.Code
}

func NewError(code int, msg string, params ...any) *Error {
	return &Error{
		Message: fmt.Sprintf(msg, params...),
		Code:    code,
	}
}

func ErrNotFound(msg string, params ...any) error {
	if msg == "" {
		msg = "not found"
	}
	return NewError(http.StatusNotFound, msg, params...)
}

func ErrBadRequest(msg string, params ...any) error {
	if msg == "" {
		msg = "bad request"
	}
	return NewError(http.StatusBadRequest, msg, params...)
}

func ErrUnAuthorized(msg string, params ...any) error {
	if msg == "" {
		msg = "user unauthorized"
	}
	return NewError(http.StatusUnauthorized, msg, params...)
}

func ErrForbidden(msg string, params ...any) error {
	if msg == "" {
		msg = "forbidden"
	}
	return NewError(http.StatusForbidden, msg, params...)
}

func ErrInternalServer(msg string, params ...any) error {
	if msg == "" {
		msg = "internal server error"
	}
	return NewError(http.StatusInternalServerError, msg, params...)
}

func ErrStatusCreated(msg string, params ...any) error {
	if msg == "" {
		msg = "created"
	}
	return NewError(http.StatusCreated, msg, params...)
}
