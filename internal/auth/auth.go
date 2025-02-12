package auth

import (
	"log/slog"
	"os"
	"time"

	"github.com/santhoshm25/key-value-ds/internal/db"
	"github.com/santhoshm25/key-value-ds/types"
	"github.com/santhoshm25/key-value-ds/utils"

	"github.com/golang-jwt/jwt"
	"golang.org/x/crypto/bcrypt"
)

const (
	DefaultProvisionedCapacity = 1073741824 // 1GB
)

var jwtKey = []byte(os.Getenv("JWT_SECRET_KEY"))

type Claims struct {
	UserID int64 `json:"user_id"`
	jwt.StandardClaims
}

func Register(db db.Database, user *types.User) error {
	if user.Name == "" || user.Password == "" {
		return utils.ErrBadRequest(utils.InvalidCredErr)
	}

	if user.ProvisionedCapacity == 0 {
		user.ProvisionedCapacity = DefaultProvisionedCapacity
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(user.Password), bcrypt.DefaultCost)
	if err != nil {
		slog.Error("error generating hashed password", "error", err)
		return utils.ErrInternalServer("")
	}

	user.Password = string(hashedPassword)
	return db.CreateUser(user)
}

func Login(db db.Database, user *types.User) (map[string]string, error) {
	if user.Name == "" || user.Password == "" {
		return nil, utils.ErrBadRequest(utils.InvalidCredErr)
	}

	userRec, err := db.GetUser(user.Name)
	if err != nil {
		return nil, utils.ErrNotFound(utils.UserNotFoundErr)
	}

	err = bcrypt.CompareHashAndPassword([]byte(userRec.Password), []byte(user.Password))
	if err != nil {
		slog.Error("error comparing hash and password", "error", err)
		return nil, utils.ErrBadRequest(utils.InvalidCredErr)
	}

	expirationTime := time.Now().Add(1 * time.Hour)
	claims := &Claims{
		UserID: userRec.ID,
		StandardClaims: jwt.StandardClaims{
			ExpiresAt: expirationTime.Unix(),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(jwtKey)
	if err != nil {
		slog.Error("error signing token", "error", err)
		return nil, utils.ErrInternalServer("")
	}
	return map[string]string{"token": tokenString}, nil
}

func ValidateToken(tokenString string) (*Claims, error) {
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		return jwtKey, nil
	})
	if err != nil {
		slog.Error("error parsing token", "error", err)
		return nil, utils.ErrInternalServer("")
	}

	var ok bool
	if claims, ok = token.Claims.(*Claims); !ok || !token.Valid {
		slog.Error("invalid token", "error", err)
		return nil, utils.ErrUnAuthorized("")
	}
	return claims, nil
}
