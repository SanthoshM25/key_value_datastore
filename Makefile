.PHONY: build
build:
	rm -f datastore
	go build -o datastore main.go

.PHONY: run
run: build 
	./datastore

.PHONY: test
test: clean migrate
	go test -v ./tests/... 

.PHONY: clean
clean:
	cd ./internal/db/schema && flyway clean
	
.PHONY: migrate
migrate:
	cd ./internal/db/schema && flyway migrate
