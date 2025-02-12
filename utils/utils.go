package utils

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"

	"github.com/joho/godotenv"
)

func ExtractRequestBody(reqBody io.ReadCloser, bodyObj any) error {
	if reqBody == nil {
		return ErrBadRequest(EmptyBodyErr)
	}

	defer reqBody.Close()

	if body, err := io.ReadAll(reqBody); err != nil {
		slog.Error("error reading request body", "error", err)
		return ErrBadRequest(InvalidBodyErr)
	} else if len(body) == 0 {
		return ErrBadRequest(EmptyBodyErr)
	} else {
		if err := json.Unmarshal(body, &bodyObj); err != nil {
			slog.Error("error unmarshalling request body", "error", err)
			return ErrBadRequest(InvalidBodyErr)
		}
	}
	return nil
}

func MarshalResponse(resp any) ([]byte, error) {
	switch resp := resp.(type) {
	case []byte:
		return resp, nil
	default:
		return json.Marshal(resp)
	}
}

func InitEnv() {
	err := godotenv.Load()
	if err != nil {
		slog.Error("error loading environment variables", "error", err)
		os.Exit(1)
	}

	envVars := []string{"DATABASE_URL", "JWT_SECRET_KEY"}

	for _, envVar := range envVars {
		if os.Getenv(envVar) == "" {
			slog.Error("environment variable is not set", "envVar", envVar)
			os.Exit(1)
		}
	}
}
