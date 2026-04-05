# ==============================================================================
# Stage 1: Build
# ==============================================================================
FROM golang:1.23-alpine AS builder

RUN apk add --no-cache \
    git \
    protobuf \
    protobuf-dev \
    make

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.2 && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

WORKDIR /app

COPY . .

RUN protoc --go_out=. --go_opt=paths=source_relative \
           --go-grpc_out=. --go-grpc_opt=paths=source_relative \
           proto/search_service.proto

RUN go mod tidy

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o /bin/search-service \
    ./cmd

# ==============================================================================
# Stage 2: Runtime
# ==============================================================================
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata && \
    adduser -D -g '' appuser

WORKDIR /app

COPY --from=builder /bin/search-service .
COPY --from=builder /app/domain-config.json ./domain-config.json

USER appuser

EXPOSE 50055

ENTRYPOINT ["./search-service"]
