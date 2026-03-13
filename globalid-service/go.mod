module github.com/quantara/globalid-service

go 1.23

require (
	github.com/lib/pq v1.10.9
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.54.0
	go.opentelemetry.io/otel v1.29.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.29.0
	go.opentelemetry.io/otel/sdk v1.29.0
	go.opentelemetry.io/otel/trace v1.29.0
	google.golang.org/grpc v1.66.0
	google.golang.org/protobuf v1.34.2
)
