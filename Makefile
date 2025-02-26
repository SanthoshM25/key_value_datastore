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
	cd ./internal/db/schema && flyway clean -url=$(URL) -user=$(USER) -password=$(PASSWORD) 
	
.PHONY: migrate
migrate:
	cd ./internal/db/schema && flyway migrate -url=$(URL) -user=$(USER) -password=$(PASSWORD)

.PHONY: prepare
prepare: 
	make clean 
	make migrate
