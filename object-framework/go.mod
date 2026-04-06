module github.com/quantara/object-framework

go 1.23

require (
	github.com/google/uuid v1.6.0
	github.com/lib/pq v1.10.9
	github.com/segmentio/kafka-go v0.4.47
	github.com/testcontainers/testcontainers-go v0.34.0
	github.com/testcontainers/testcontainers-go/modules/kafka v0.34.0
	github.com/testcontainers/testcontainers-go/modules/postgres v0.34.0
	github.com/testcontainers/testcontainers-go/modules/redis v0.34.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.54.0
	go.opentelemetry.io/otel v1.29.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.29.0
	go.opentelemetry.io/otel/sdk v1.29.0
	go.opentelemetry.io/otel/trace v1.29.0
	google.golang.org/grpc v1.66.0
	google.golang.org/protobuf v1.34.2
)
