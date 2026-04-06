.PHONY: proto build run test clean test-e2e test-e2e-verbose

PROTO_DIR := proto
GO_OUT := .

proto:
	protoc --go_out=$(GO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(GO_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/idservice/id_service.proto
	protoc --go_out=$(GO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(GO_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/globalidservice/globalid_service.proto
	protoc --go_out=$(GO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(GO_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/lockservice/lock_service.proto
	protoc --go_out=$(GO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(GO_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/searchservice/search_service.proto
	protoc --go_out=$(GO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(GO_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/transaction/transaction_service.proto

build: proto
	go build -o bin/object-framework ./cmd

run: build
	./bin/object-framework

test:
	go test -v ./...

test-unit:
	go test -v -short ./...

test-integration:
	go test -v -tags=integration ./...

# E2E tests - requires full docker-compose stack running
# Run from parent directory: docker-compose up -d
test-e2e:
	@echo "Running E2E tests against local docker-compose stack..."
	@echo "Make sure the full stack is running: cd .. && docker-compose up -d"
	go test -v -tags=e2e -timeout=120s ./test/e2e/...

test-e2e-verbose:
	@echo "Running E2E tests with verbose output..."
	go test -v -tags=e2e -timeout=120s -count=1 ./test/e2e/...

# Run all tests (unit + integration + e2e)
test-all: test-unit test-integration test-e2e

clean:
	rm -rf bin/

docker-build:
	docker build -t object-framework:latest .

# Docker-based testing (builds and runs tests in container)
docker-test:
	docker-compose -f docker-compose.test.yml up --build --abort-on-container-exit test
	docker-compose -f docker-compose.test.yml down -v

lint:
	golangci-lint run ./...
