package telemetry

import (
	"context"
	"log"
	"os"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// InitTracer initializes OpenTelemetry tracer with parent-based ratio sampling.
// Sampling ratio is configurable via OTEL_TRACES_SAMPLER_RATIO (default 0.1 = 10%).
// If upstream sets sampling decision in traceparent, we honor it (ParentBased).
func InitTracer(ctx context.Context, serviceName, jaegerEndpoint string) (func(context.Context) error, error) {
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(jaegerEndpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, err
	}

	ratio := 0.1
	if v := os.Getenv("OTEL_TRACES_SAMPLER_RATIO"); v != "" {
		if parsed, err := strconv.ParseFloat(v, 64); err == nil && parsed >= 0 && parsed <= 1 {
			ratio = parsed
		}
	}
	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	log.Printf("Telemetry initialized, exporting to %s, sampler=ParentBased(ratio=%.2f)", jaegerEndpoint, ratio)

	return tp.Shutdown, nil
}
