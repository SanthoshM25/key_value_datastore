.PHONY: build
build:
	GOOS=linux go build -o datastore main.go


.PHONY: test
test:
	go test -v ./tests/...