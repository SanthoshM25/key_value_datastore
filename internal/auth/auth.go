package auth

import (
	"fmt"
	"time"

	"github.com/santhoshm25/key-value-ds/internal/db"
	"github.com/santhoshm25/key-value-ds/types"

	"github.com/golang-jwt/jwt"
	"golang.org/x/crypto/bcrypt"
)

// TODO: use env
var jwtKey = []byte("my_secret_key")

type Claims struct {
	UserID int64 `json:"user_id"`
	jwt.StandardClaims
}

func Register(db db.Database, user *types.User) error {
	if user.Name == "" || user.Password == "" {
		return fmt.Errorf("username and password cannot be empty")
	}

	if user.ProvisionedCapacity == 0 {
		user.ProvisionedCapacity = 1073741824
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(user.Password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	user.Password = string(hashedPassword)
	if err := db.CreateUser(user); err != nil {
		return err
	}
	return nil
}

func Login(db db.Database, user *types.User) (string, error) {
	if user.Name == "" || user.Password == "" {
		return "", fmt.Errorf("username and password cannot be empty")
	}
	userRec, err := db.GetUser(user.Name)
	if err != nil {
		return "", fmt.Errorf("user not found, %s", err.Error())
	}
	err = bcrypt.CompareHashAndPassword([]byte(userRec.Password), []byte(user.Password))
	if err != nil {
		return "", fmt.Errorf("password mismatch %s", err.Error())
	}
	expirationTime := time.Now().Add(1 * time.Hour)
	claims := &Claims{
		UserID: userRec.ID,
		StandardClaims: jwt.StandardClaims{
			ExpiresAt: expirationTime.Unix(),
		},
	}
	//TODO: check if the signing algo is fine or should go with SHA256
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(jwtKey)
	if err != nil {
		return "", fmt.Errorf("error in generating token: %s", err.Error())
	}
	return tokenString, nil
}

func ValidateToken(tokenString string) (*Claims, error) {
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		return jwtKey, nil
	})
	if err != nil {
		return nil, fmt.Errorf("error in parsing token: %s", err.Error())
	}
	var ok bool
	if claims, ok = token.Claims.(*Claims); !ok || !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}
	return claims, nil
}
