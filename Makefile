.PHONY: all build up down logs clean proto

# Build all services
build:
	docker-compose build

# Start all services
up:
	docker-compose up -d

# Start all services with build
up-build:
	docker-compose up -d --build

# Stop all services
down:
	docker-compose down

# Stop all services and remove volumes
down-v:
	docker-compose down -v

# View logs
logs:
	docker-compose logs -f

# View logs for specific service
logs-id:
	docker-compose logs -f id-service

logs-globalid:
	docker-compose logs -f globalid-service

logs-postgres:
	docker-compose logs -f postgres

logs-jaeger:
	docker-compose logs -f jaeger

# Clean up
clean:
	docker-compose down -v --rmi all

# Generate proto for all services
proto:
	cd id-service && make proto
	cd globalid-service && make proto

# Start infrastructure only
infra-up:
	docker-compose up -d postgres jaeger

# Stop infrastructure
infra-down:
	docker-compose stop postgres jaeger

# Health check
health:
	@echo "Checking PostgreSQL..."
	@docker-compose exec postgres pg_isready -U postgres || echo "PostgreSQL is not ready"
	@echo "Checking Jaeger..."
	@curl -s http://localhost:16686 > /dev/null && echo "Jaeger is ready" || echo "Jaeger is not ready"

# Test connectivity to services
test-grpc:
	@echo "Testing ID Service..."
	@grpcurl -plaintext localhost:50051 list || echo "ID Service is not available"
	@echo "Testing GlobalID Service..."
	@grpcurl -plaintext localhost:50052 list || echo "GlobalID Service is not available"
