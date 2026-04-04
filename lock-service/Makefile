.PHONY: proto build run test clean

PROTO_DIR := proto
GO_OUT := .

proto:
	protoc --go_out=$(GO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(GO_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/lock_service.proto

build: proto
	go build -o bin/lock-service ./cmd

run: build
	./bin/lock-service

test:
	go test -v ./...

clean:
	rm -rf bin/

docker-build:
	docker build -t lock-service:latest .

lint:
	golangci-lint run ./...
