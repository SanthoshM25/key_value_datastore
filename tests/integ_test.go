package integ_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/santhoshm25/key-value-ds/types"
)

const (
	baseURL     = "http://localhost:8080"
	contentType = "application/json"
)

func TestIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Suite")
}

var _ = Describe("Integration Tests", func() {

	// Helper Functions

	registerUser := func(name, password string, provisionedCapacity int64) *http.Response {
		user := types.User{
			Name:                name,
			Password:            password,
			ProvisionedCapacity: provisionedCapacity,
		}
		body, err := json.Marshal(user)
		Expect(err).To(BeNil())

		resp, err := http.Post(
			fmt.Sprintf("%s/api/auth/register", baseURL),
			contentType,
			bytes.NewBuffer(body),
		)
		Expect(err).To(BeNil())
		return resp
	}

	// parseToken reads the response body and extracts the JWT token.
	parseToken := func(resp *http.Response) string {
		defer resp.Body.Close()
		var loginResp map[string]string
		err := json.NewDecoder(resp.Body).Decode(&loginResp)
		Expect(err).To(BeNil())
		token, ok := loginResp["token"]
		Expect(ok).To(BeTrue())
		Expect(token).NotTo(BeEmpty())
		return token
	}

	loginUser := func(name, password string) (string, *http.Response) {
		user := types.User{
			Name:     name,
			Password: password,
		}
		body, err := json.Marshal(user)
		Expect(err).To(BeNil())
		resp, err := http.Post(
			fmt.Sprintf("%s/api/auth/login", baseURL),
			contentType,
			bytes.NewBuffer(body),
		)
		Expect(err).To(BeNil())
		if resp.StatusCode != http.StatusOK {
			return "", resp
		}
		return parseToken(resp), resp
	}

	createObject := func(token, key string, value any, ttl int64) *http.Response {
		obj := types.Object{
			Key:   key,
			Value: value,
			TTL:   ttl,
		}
		body, err := json.Marshal(obj)
		Expect(err).To(BeNil())
		req, err := http.NewRequest(http.MethodPost,
			fmt.Sprintf("%s/api/object", baseURL),
			bytes.NewBuffer(body))
		Expect(err).To(BeNil())
		req.Header.Set("Content-Type", contentType)
		req.Header.Set("Authorization", token)
		resp, err := http.DefaultClient.Do(req)
		Expect(err).To(BeNil())
		return resp
	}

	getObject := func(token, key string) (types.Object, *http.Response) {
		req, err := http.NewRequest(http.MethodGet,
			fmt.Sprintf("%s/api/object/%s", baseURL, key), nil)
		Expect(err).To(BeNil())
		req.Header.Set("Authorization", token)
		resp, err := http.DefaultClient.Do(req)
		Expect(err).To(BeNil())
		var obj types.Object
		if resp.StatusCode == http.StatusOK {
			err = json.NewDecoder(resp.Body).Decode(&obj)
			Expect(err).To(BeNil())
		}
		return obj, resp
	}

	deleteObject := func(token, key string) *http.Response {
		req, err := http.NewRequest(http.MethodDelete,
			fmt.Sprintf("%s/api/object/%s", baseURL, key), nil)
		Expect(err).To(BeNil())
		req.Header.Set("Authorization", token)
		resp, err := http.DefaultClient.Do(req)
		Expect(err).To(BeNil())
		return resp
	}

	Describe("User Registration and Login", func() {
		const defaultCapacity = 1073741824 // 1GB
		Context("When a new user registers", func() {
			It("should register and log in successfully", func() {
				resp := registerUser("normalUser", "normalPass", defaultCapacity)
				Expect(resp.StatusCode).To(Equal(http.StatusCreated))

				token, loginResp := loginUser("normalUser", "normalPass")
				Expect(loginResp.StatusCode).To(Equal(http.StatusOK))
				Expect(token).NotTo(BeEmpty())
			})
		})

		Context("When a duplicate user registers", func() {
			It("should fail with a duplicate user error", func() {
				// First registration should succeed.
				resp1 := registerUser("dupUser", "dupPass", defaultCapacity)
				Expect(resp1.StatusCode).To(Equal(http.StatusCreated))

				// Second registration (with the same credentials) should fail.
				resp2 := registerUser("dupUser", "dupPass", defaultCapacity)
				// Expect 400 Bad Request for duplicate registration.
				Expect(resp2.StatusCode).To(Equal(http.StatusBadRequest))
				bodyBytes, err := io.ReadAll(resp2.Body)
				Expect(err).To(BeNil())
				resp2.Body.Close()
				Expect(string(bodyBytes)).To(ContainSubstring("user already exists"))
			})
		})

		Context("When logging in with invalid credentials", func() {
			It("should fail to log in", func() {
				// Register a valid user.
				resp := registerUser("invalidLoginUser", "correctPass", defaultCapacity)
				Expect(resp.StatusCode).To(Equal(http.StatusCreated))

				// Attempt to log in with an incorrect password.
				_, loginResp := loginUser("invalidLoginUser", "wrongPass")
				// Expect 400 Bad Request due to invalid credentials.
				Expect(loginResp.StatusCode).To(Equal(http.StatusBadRequest))
			})
		})
	})

	Describe("Object Operations", func() {
		var token string
		It("should register and log in successfully", func() {
			user := "objectUser"
			pass := "objectPass"
			resp := registerUser(user, pass, 1073741824)
			Expect(resp.StatusCode).To(Equal(http.StatusCreated))
			tkn, _ := loginUser(user, pass)
			token = tkn
		})

		Context("Normal operations", func() {
			It("should create, retrieve and delete an object successfully", func() {
				valueMap := map[string]any{
					"stringValue": "text",
					"numberValue": float64(42),
					"boolValue":   true,
					"floatValue":  12.34,
					"nested":      map[string]any{"innerKey": "innerValue"},
					"array":       []any{float64(1), "two", false},
				}
				respCreate := createObject(token, "objKey", valueMap, 1739365812)
				Expect(respCreate.StatusCode).To(Equal(http.StatusCreated))

				// Retrieve the object.
				obj, respGet := getObject(token, "objKey")
				Expect(respGet.StatusCode).To(Equal(http.StatusOK))
				Expect(obj.Key).To(Equal("objKey"))

				// Perform a direct map comparison by type asserting and checking each key.
				actualMap, ok := obj.Value.(map[string]any)
				Expect(ok).To(BeTrue(), "Expected obj.Value to be of type map[string]any")
				// Check that all expected keys exist and match
				for k, expVal := range valueMap {
					actVal, exists := actualMap[k]
					Expect(exists).To(BeTrue(), fmt.Sprintf("Key %q is missing in the actual map", k))
					Expect(actVal).To(Equal(expVal), fmt.Sprintf("Mismatch for key %q", k))
				}
				// Check that there are no extra keys.
				Expect(len(actualMap)).To(Equal(len(valueMap)), "Actual map has extra keys")

				// Delete the object.
				respDel := deleteObject(token, "objKey")
				Expect(respDel.StatusCode).To(Equal(http.StatusNoContent))
			})
		})

		Context("Exceeding allowed limits", func() {
			It("should fail to create an object with a key that exceeds the allowed length", func() {
				// Generate a key longer than 32 characters.
				longKey := strings.Repeat("a", 33)
				resp := createObject(token, longKey, "value", 1739365812)
				// Expect the operation not to succeed.
				Expect(resp.StatusCode).ToNot(Equal(http.StatusCreated))
				bodyBytes, err := io.ReadAll(resp.Body)
				Expect(err).To(BeNil())
				resp.Body.Close()
				Expect(string(bodyBytes)).To(ContainSubstring("key"))
			})

			It("should fail to create an object with a value that exceeds the allowed size", func() {
				// Generate a string longer than 16KB (16384 bytes) for the value.
				longValue := strings.Repeat("v", 16385)
				resp := createObject(token, "validKey", longValue, 1739365812)
				Expect(resp.StatusCode).ToNot(Equal(http.StatusCreated))
				bodyBytes, err := io.ReadAll(resp.Body)
				Expect(err).To(BeNil())
				resp.Body.Close()
				Expect(string(bodyBytes)).To(ContainSubstring("value"))
			})
		})
	})

	Describe("Quota Enforcement", func() {
		Context("When a user's provisioned capacity is lower", func() {
			It("should fail to create an object that exceeds the quota", func() {
				// Register a user with a very low capacity (e.g., 100 bytes).
				resp := registerUser("quotaUser", "quotaPass", 100)
				Expect(resp.StatusCode).To(Equal(http.StatusCreated))
				token, _ := loginUser("quotaUser", "quotaPass")

				// Attempt to create an object whose value size exceeds the small quota.
				largeValue := strings.Repeat("x", 200)
				respCreate := createObject(token, "quotaTestKey", largeValue, 1739365812)
				// Expect 400 Bad Request because the quota is exceeded.
				Expect(respCreate.StatusCode).To(Equal(http.StatusBadRequest))
				bodyBytes, err := io.ReadAll(respCreate.Body)
				Expect(err).To(BeNil())
				respCreate.Body.Close()
				Expect(string(bodyBytes)).To(ContainSubstring("quota exceeded"))
			})
		})
	})

	Describe("Batch Operations", func() {
		var token string
		userCount := 0
		userCountStr := strconv.Itoa(userCount)
		BeforeEach(func() {
			userCountStr = strconv.Itoa(userCount)
			resp := registerUser("batchUser"+userCountStr, "batchPass", 1073741824)
			Expect(resp.StatusCode).To(Equal(http.StatusCreated))
			tkn, _ := loginUser("batchUser"+userCountStr, "batchPass")
			token = tkn
			userCount++
		})

		It("should create and retrieve objects in a batch", func() {
			objects := []types.Object{
				{
					Key:   "batch-key-1",
					Value: map[string]any{"field": "value1", "num": float64(1), "bool": true},
					TTL:   1739365812,
				},
				{
					Key:   "batch-key-2",
					Value: map[string]any{"field": "value2", "num": float64(2), "bool": false},
					TTL:   1739365812,
				},
			}
			body, err := json.Marshal(objects)
			Expect(err).To(BeNil())
			req, err := http.NewRequest(http.MethodPost,
				fmt.Sprintf("%s/api/batch/object", baseURL),
				bytes.NewBuffer(body))
			Expect(err).To(BeNil())
			req.Header.Set("Content-Type", contentType)
			req.Header.Set("Authorization", token)
			resp, err := http.DefaultClient.Do(req)
			Expect(err).To(BeNil())
			defer resp.Body.Close()
			Expect(resp.StatusCode).To(Equal(http.StatusCreated))

			// Verify each object in the batch.
			for _, obj := range objects {
				retrievedObj, respGet := getObject(token, obj.Key)
				Expect(respGet.StatusCode).To(Equal(http.StatusOK))
				Expect(retrievedObj.Key).To(Equal(obj.Key))
				actualMap, ok := retrievedObj.Value.(map[string]any)
				Expect(ok).To(BeTrue(), fmt.Sprintf("Expected obj.Value for key %s to be map[string]any", obj.Key))
				expectedMap, ok := obj.Value.(map[string]any)
				Expect(ok).To(BeTrue())
				for k, expVal := range expectedMap {
					actVal, exists := actualMap[k]
					Expect(exists).To(BeTrue(), fmt.Sprintf("Key %q missing for object %s", k, obj.Key))
					Expect(actVal).To(Equal(expVal), fmt.Sprintf("Mismatch for key %q in object %s", k, obj.Key))
				}
				Expect(len(actualMap)).To(Equal(len(expectedMap)), fmt.Sprintf("Unexpected extra keys in object %s", obj.Key))
			}
		})

		Context("Combined Value Size Limits in Batch", func() {
			It("should fail to create a batch request if the combined values exceed 4MB", func() {
				largeStr1 := strings.Repeat("B", 3145728) // ~3MB
				largeStr2 := strings.Repeat("C", 3145728) // ~3MB
				obj1 := types.Object{
					Key:   "combinedObj1",
					Value: map[string]any{"data": largeStr1},
					TTL:   1739365812,
				}
				obj2 := types.Object{
					Key:   "combinedObj2",
					Value: map[string]any{"data": largeStr2},
					TTL:   1739365812,
				}
				objects := []types.Object{obj1, obj2}
				body, err := json.Marshal(objects)
				Expect(err).To(BeNil())

				req, err := http.NewRequest(http.MethodPost,
					fmt.Sprintf("%s/api/batch/object", baseURL),
					bytes.NewBuffer(body))
				Expect(err).To(BeNil())
				req.Header.Set("Content-Type", contentType)
				req.Header.Set("Authorization", token)

				resp, err := http.DefaultClient.Do(req)
				Expect(err).To(BeNil())
				defer resp.Body.Close()

				Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
			})

			It("should succeed to create a batch request if the combined values are slightly below 4MB", func() {
				validStr1 := strings.Repeat("D", 2097100)
				validStr2 := strings.Repeat("E", 2097100)
				obj1 := types.Object{
					Key:   "combinedObj3",
					Value: map[string]any{"data": validStr1},
					TTL:   1739365812,
				}
				obj2 := types.Object{
					Key:   "combinedObj4",
					Value: map[string]any{"data": validStr2},
					TTL:   1739365812,
				}
				objects := []types.Object{obj1, obj2}
				body, err := json.Marshal(objects)
				Expect(err).To(BeNil())

				req, err := http.NewRequest(http.MethodPost,
					fmt.Sprintf("%s/api/batch/object", baseURL),
					bytes.NewBuffer(body))
				Expect(err).To(BeNil())
				req.Header.Set("Content-Type", contentType)
				req.Header.Set("Authorization", token)

				resp, err := http.DefaultClient.Do(req)
				Expect(err).To(BeNil())
				defer resp.Body.Close()

				// The combined data size is slightly below 4MB.
				Expect(resp.StatusCode).To(Equal(http.StatusCreated))

				// Verify each object is retrievable.
				for _, obj := range objects {
					retrievedObj, respGet := getObject(token, obj.Key)
					Expect(respGet.StatusCode).To(Equal(http.StatusOK))
					actualMap, ok := retrievedObj.Value.(map[string]any)
					Expect(ok).To(BeTrue(), fmt.Sprintf("Expected obj.Value for key %s to be map[string]any", obj.Key))
					expectedMap, ok := obj.Value.(map[string]any)
					Expect(ok).To(BeTrue())
					for k, expVal := range expectedMap {
						actVal, exists := actualMap[k]
						Expect(exists).To(BeTrue(), fmt.Sprintf("Key %q missing for object %s", k, obj.Key))
						Expect(actVal).To(Equal(expVal), fmt.Sprintf("Mismatch for key %q in object %s", k, obj.Key))
					}
					Expect(len(actualMap)).To(Equal(len(expectedMap)))
				}
			})
		})
	})
})
