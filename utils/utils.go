package utils

import (
	"encoding/json"
	"fmt"
	"io"
)

func ExtractRequestBody(reqBody io.ReadCloser, bodyObj any) error {
	if reqBody == nil {
		return fmt.Errorf("empty request body")
	}
	defer reqBody.Close()
	if body, err := io.ReadAll(reqBody); err != nil {
		return fmt.Errorf("error reading request body: %v", err)
	} else if len(body) == 0 {
		return fmt.Errorf("empty request body")
	} else {
		if err := json.Unmarshal(body, &bodyObj); err != nil {
			return fmt.Errorf("error unmarshalling request body: %v", err)
		}
		return nil
	}
}
