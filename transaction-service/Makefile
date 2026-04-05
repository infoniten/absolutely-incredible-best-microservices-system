.PHONY: proto build run test clean

PROTO_DIR := proto
GO_OUT := .

proto:
	protoc --go_out=$(GO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(GO_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/transaction_service.proto

build: proto
	go build -o bin/transaction-service ./cmd

run: build
	./bin/transaction-service

test:
	go test -v ./...

clean:
	rm -rf bin/

docker-build:
	docker build -t transaction-service:latest .

lint:
	golangci-lint run ./...
