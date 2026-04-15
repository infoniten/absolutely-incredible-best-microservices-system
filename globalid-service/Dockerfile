# ==============================================================================
# Stage 1: Build
# ==============================================================================
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache \
    git \
    protobuf \
    protobuf-dev \
    make

# Install protoc plugins (fixed versions compatible with Go 1.23)
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.2 && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

WORKDIR /app

# Copy all source code
COPY . .

# Generate protobuf files first (before go mod tidy)
RUN protoc --go_out=. --go_opt=paths=source_relative \
           --go-grpc_out=. --go-grpc_opt=paths=source_relative \
           proto/globalid_service.proto

# Download dependencies
RUN go mod tidy

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o /bin/globalid-service \
    ./cmd

# ==============================================================================
# Stage 2: Runtime
# ==============================================================================
FROM alpine:3.19

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata && \
    adduser -D -g '' appuser

WORKDIR /app

# Copy binary from builder
COPY --from=builder /bin/globalid-service .

# Use non-root user
USER appuser

EXPOSE 50052

ENTRYPOINT ["./globalid-service"]
