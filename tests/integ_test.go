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
	"time"

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

	cleanup := func(token, key string) {
		req, err := http.NewRequest(http.MethodDelete,
			fmt.Sprintf("%s/api/object/%s", baseURL, key),
			nil)
		if err != nil {
			return
		}
		req.Header.Set("Authorization", token)
		http.DefaultClient.Do(req)
		// Intentionally ignore errors as this is just cleanup
	}

	cleanupTestData := func() {
		// Create a test user for cleanup
		resp := registerUser("cleanupUser", "cleanupPass", 1073741824)
		if resp.StatusCode != http.StatusCreated {
			return
		}
		token, _ := loginUser("cleanupUser", "cleanupPass")
		if token == "" {
			return
		}

		// Clean up known test objects
		testKeys := []string{
			"objKey",
			"batch-key-1",
			"batch-key-2",
			"combinedObj1",
			"combinedObj2",
			"combinedObj3",
			"combinedObj4",
			"short-ttl-key",
			"past-ttl-key",
			"future-ttl-key",
			"batch-ttl-short",
			"batch-ttl-long",
			"batch-past-ttl-1",
			"batch-past-ttl-2",
		}

		for _, key := range testKeys {
			cleanup(token, key)
		}
	}

	getTTL := func() int64 {
		return time.Now().Add(time.Hour * 24).Unix()
	}

	BeforeEach(func() {
		cleanupTestData()
	})

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
			resp := registerUser(user, pass, getTTL())
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
				respCreate := createObject(token, "objKey", valueMap, getTTL())
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
				resp := createObject(token, longKey, "value", getTTL())
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
				resp := createObject(token, "validKey", longValue, getTTL())
				Expect(resp.StatusCode).ToNot(Equal(http.StatusCreated))
				bodyBytes, err := io.ReadAll(resp.Body)
				Expect(err).To(BeNil())
				resp.Body.Close()
				Expect(string(bodyBytes)).To(ContainSubstring("value"))
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
					respCreate := createObject(token, "quotaTestKey", largeValue, getTTL())
					// Expect 403 Forbidden because the quota is exceeded.
					Expect(respCreate.StatusCode).To(Equal(http.StatusForbidden))
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
						TTL:   getTTL(),
					},
					{
						Key:   "batch-key-2",
						Value: map[string]any{"field": "value2", "num": float64(2), "bool": false},
						TTL:   getTTL(),
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
						TTL:   getTTL(),
					}
					obj2 := types.Object{
						Key:   "combinedObj2",
						Value: map[string]any{"data": largeStr2},
						TTL:   getTTL(),
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
						TTL:   getTTL(),
					}
					obj2 := types.Object{
						Key:   "combinedObj4",
						Value: map[string]any{"data": validStr2},
						TTL:   getTTL(),
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

			Context("TTL behavior", func() {
				It("should expire objects after TTL has passed", func() {
					// Create an object with a short TTL (2 seconds)
					shortTTL := time.Now().Add(2 * time.Second).Unix()
					value := map[string]any{
						"data": "This should expire soon",
					}

					respCreate := createObject(token, "short-ttl-key", value, shortTTL)
					Expect(respCreate.StatusCode).To(Equal(http.StatusCreated))

					// Verify object is accessible immediately
					obj, respGet := getObject(token, "short-ttl-key")
					Expect(respGet.StatusCode).To(Equal(http.StatusOK))
					Expect(obj.Key).To(Equal("short-ttl-key"))

					// Wait for TTL to expire (3 seconds to be safe)
					time.Sleep(3 * time.Second)

					// Verify object is no longer accessible
					_, respGetExpired := getObject(token, "short-ttl-key")
					Expect(respGetExpired.StatusCode).To(Equal(http.StatusNotFound))
				})

				It("should reject objects with past TTL", func() {
					pastTTL := time.Now().Add(-1 * time.Hour).Unix()
					value := map[string]any{
						"data": "This should not be accepted",
					}

					respCreate := createObject(token, "past-ttl-key", value, pastTTL)
					Expect(respCreate.StatusCode).To(Equal(http.StatusBadRequest))
				})

				It("should accept and maintain objects with future TTL", func() {
					futureTTL := time.Now().Add(24 * time.Hour).Unix()
					value := map[string]any{
						"data": "This should stay accessible",
					}

					respCreate := createObject(token, "future-ttl-key", value, futureTTL)
					Expect(respCreate.StatusCode).To(Equal(http.StatusCreated))

					// Verify object is accessible
					obj, respGet := getObject(token, "future-ttl-key")
					Expect(respGet.StatusCode).To(Equal(http.StatusOK))
					Expect(obj.Key).To(Equal("future-ttl-key"))
					Expect(obj.TTL).To(Equal(futureTTL))
				})

				It("should handle batch operations with mixed TTLs correctly", func() {
					shortTTL := time.Now().Add(2 * time.Second).Unix()
					futureTTL := time.Now().Add(24 * time.Hour).Unix()

					objects := []types.Object{
						{
							Key:   "batch-ttl-short",
							Value: map[string]any{"data": "expires soon"},
							TTL:   shortTTL,
						},
						{
							Key:   "batch-ttl-long",
							Value: map[string]any{"data": "expires later"},
							TTL:   futureTTL,
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
					Expect(resp.StatusCode).To(Equal(http.StatusCreated))
					resp.Body.Close()

					// Wait for short TTL to expire
					time.Sleep(3 * time.Second)

					// Verify short TTL object has expired
					_, respGetShort := getObject(token, "batch-ttl-short")
					Expect(respGetShort.StatusCode).To(Equal(http.StatusNotFound))

					// Verify long TTL object is still accessible
					objLong, respGetLong := getObject(token, "batch-ttl-long")
					Expect(respGetLong.StatusCode).To(Equal(http.StatusOK))
					Expect(objLong.Key).To(Equal("batch-ttl-long"))
				})

				It("should reject batch operations with past TTLs", func() {
					pastTTL := time.Now().Add(-1 * time.Hour).Unix()
					objects := []types.Object{
						{
							Key:   "batch-past-ttl-1",
							Value: map[string]any{"data": "should not work"},
							TTL:   pastTTL,
						},
						{
							Key:   "batch-past-ttl-2",
							Value: map[string]any{"data": "should not work either"},
							TTL:   pastTTL,
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
					Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
					resp.Body.Close()
				})
			})
		})
	})

	Describe("Concurrency Tests", func() {
		var userTokens []string
		const numUsers = 200
		const numOperations = 10

		func() {
			userTokens = make([]string, numUsers)
			for i := range numUsers {
				username := fmt.Sprintf("concurrency_user_%d", i)
				password := fmt.Sprintf("pass_%d", i)

				resp := registerUser(username, password, getTTL())
				Expect(resp.StatusCode).To(Equal(http.StatusCreated))

				token, loginResp := loginUser(username, password)
				Expect(loginResp.StatusCode).To(Equal(http.StatusOK))
				Expect(token).NotTo(BeEmpty())

				userTokens[i] = token
			}
		}()

		AfterEach(func() {
			for _, token := range userTokens {
				for i := range numOperations {
					cleanup(token, fmt.Sprintf("concurrent_key_%d", i))
					cleanup(token, "shared_key")
				}
			}
		})

		// Multiple users writing
		It("should handle multiple users writing concurrently", func() {
			// Create channels to track operations
			done := make(chan bool, numUsers)
			results := make(chan int, numUsers)

			// Creating objects from multiple users
			for i := range numUsers {
				go func(idx int) {
					token := userTokens[idx]
					resp := createObject(token, "shared_key", map[string]any{
						"writer": idx,
						"time":   time.Now().Unix(),
					}, getTTL())

					results <- resp.StatusCode
					done <- true
				}(i)
			}

			// Wait for all operations to complete
			for range numUsers {
				<-done
			}

			successCount := 0
			close(results)
			for statusCode := range results {
				if statusCode == http.StatusCreated {
					successCount++
				}
			}

			// All operation should succeed
			Expect(successCount).To(Equal(numUsers))

			// Verify the key exists
			for _, token := range userTokens {
				obj, resp := getObject(token, "shared_key")
				Expect(resp.StatusCode).To(Equal(http.StatusOK))
				Expect(obj.Key).To(Equal("shared_key"))
			}
		})

		// Single user performing multiple operations concurrently
		It("should handle same user performing multiple operations concurrently", func() {
			token := userTokens[0]
			done := make(chan bool, numOperations)

			// Creating multiple objects concurrently
			for i := range numOperations {
				go func(idx int) {
					key := fmt.Sprintf("concurrent_key_%d", idx)
					resp := createObject(token, key, map[string]any{
						"operation": idx,
						"time":      time.Now().Unix(),
					}, getTTL())

					Expect(resp.StatusCode).To(Equal(http.StatusCreated))
					done <- true
				}(i)
			}

			for range numOperations {
				<-done
			}

			// Verify all objects were created
			for i := range numOperations {
				key := fmt.Sprintf("concurrent_key_%d", i)
				obj, resp := getObject(token, key)
				Expect(resp.StatusCode).To(Equal(http.StatusOK))
				Expect(obj.Key).To(Equal(key))
			}
		})

		// Concurrent reads and writes on the same key from single user
		It("should handle concurrent reads and writes on the same key", func() {
			sharedKey := "shared_key"
			token := userTokens[0]

			// Create the initial object
			resp := createObject(token, sharedKey, map[string]any{
				"initial": true,
				"time":    time.Now().Unix(),
			}, getTTL())
			Expect(resp.StatusCode).To(Equal(http.StatusCreated))

			done := make(chan bool, numOperations*2)

			// Start concurrent reads and writes
			for i := range numOperations {
				// Writer goroutines
				go func(idx int) {
					resp := createObject(token, sharedKey, map[string]any{
						"writer": idx,
						"time":   time.Now().Unix(),
					}, getTTL())

					Expect(resp.StatusCode).To(BeElementOf([]int{http.StatusCreated, http.StatusOK}))
					done <- true
				}(i)

				// Reader goroutines
				go func() {
					obj, resp := getObject(token, sharedKey)
					Expect(resp.StatusCode).To(Equal(http.StatusOK))
					Expect(obj.Key).To(Equal(sharedKey))
					done <- true
				}()
			}

			for range numOperations * 2 {
				<-done
			}

			obj, resp := getObject(token, sharedKey)
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			Expect(obj.Key).To(Equal(sharedKey))
		})

		// Race condition test: Create objects, then concurrently delete and get them
		It("should handle race conditions between delete and get operations", func() {
			const numRaceObjects = 5
			var raceKeys []string
			token := userTokens[0]

			// Create multiple objects
			for i := range numRaceObjects {
				raceKey := fmt.Sprintf("race_object_%d", i)
				raceKeys = append(raceKeys, raceKey)
				resp := createObject(token, raceKey, map[string]any{
					"operation": "create",
					"time":      time.Now().Unix(),
				}, getTTL())
				Expect(resp.StatusCode).To(Equal(http.StatusCreated))
			}

			done := make(chan bool, numRaceObjects*2)

			// Start concurrent delete and get operations
			for _, raceKey := range raceKeys {
				go func(key string) {
					resp := deleteObject(token, key)
					// Either it was deleted successfully or not found
					Expect(resp.StatusCode).To(Equal(http.StatusNoContent))
					done <- true
				}(raceKey)

				go func(key string) {
					_, resp := getObject(token, key)
					// Either it exists or not
					Expect(resp.StatusCode).To(BeElementOf([]int{http.StatusOK, http.StatusNotFound}))
					done <- true
				}(raceKey)
			}

			// Wait for all operations to complete
			for range numRaceObjects * 2 {
				<-done
			}
		})
	})
})
