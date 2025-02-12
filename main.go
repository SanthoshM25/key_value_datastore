package main

import (
	"log"
	"net/http"

	"github.com/santhoshm25/key-value-ds/internal/db/mysql"
	"github.com/santhoshm25/key-value-ds/internal/server"

	"github.com/julienschmidt/httprouter"
)

func main() {
	msDB := mysql.NewDB()
	msDB.Init()
	defer log.Fatal(msDB.Db.Close().Error())

	router := httprouter.New()
	router.POST("/api/auth/register", server.RegisterHandler(msDB))
	router.POST("/api/auth/login", server.LoginHandler(msDB))
	router.POST("/api/object", server.AuthHandler(msDB, server.CreateObjectHandler(msDB)))
	router.GET("/api/object/:key", server.AuthHandler(msDB, server.GetObjectHandler(msDB)))
	router.DELETE("/api/object/:key", server.AuthHandler(msDB, server.DeleteObjectHandler(msDB)))
	router.POST("/api/batch/object", server.AuthHandler(msDB, server.BatchCreateObjectHandler(msDB)))

	log.Fatal(http.ListenAndServe(":8080", router))
}
