.PHONY: build
build:
	go build -o datastore main.go

.PHONY: run
run: build 
	./datastore

.PHONY: test
test:
	go test -v ./tests/... 

.PHONY: clean
clean:
	rm -f datastore

.PHONY: migrate
migrate:
	cd ./internal/db/schema && flyway migrate
