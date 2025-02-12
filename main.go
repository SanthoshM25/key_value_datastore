package main

import (
	"log"
	"log/slog"
	"net/http"

	"github.com/julienschmidt/httprouter"

	"github.com/santhoshm25/key-value-ds/internal/db/mysql"
	"github.com/santhoshm25/key-value-ds/internal/server"
	"github.com/santhoshm25/key-value-ds/utils"
)

const (
	port = ":8080"
)

func main() {
	utils.InitEnv()

	msDB := mysql.NewDB()
	msDB.Init()

	router := httprouter.New()
	router.POST("/api/auth/register", server.RegisterHandler(msDB))
	router.POST("/api/auth/login", server.LoginHandler(msDB))
	router.POST("/api/object", server.AuthHandler(msDB, server.CreateObjectHandler(msDB)))
	router.GET("/api/object/:key", server.AuthHandler(msDB, server.GetObjectHandler(msDB)))
	router.DELETE("/api/object/:key", server.AuthHandler(msDB, server.DeleteObjectHandler(msDB)))
	router.POST("/api/batch/object", server.AuthHandler(msDB, server.BatchCreateObjectHandler(msDB)))

	slog.Info("Starting server on", "port", port)
	log.Fatal(http.ListenAndServe(port, router))

	defer log.Fatal(msDB.Db.Close())
}
