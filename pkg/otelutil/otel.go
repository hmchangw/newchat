package otelutil

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// InitTracer is a no-op (propagator only) when no OTLP endpoint env var is
// set, to avoid `traces export: connection refused` spam in dev / CI.
func InitTracer(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		otel.SetTextMapPropagator(propagation.TraceContext{})
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceNameKey.String(serviceName),
	))
	if err != nil {
		return nil, fmt.Errorf("resource: %w", err)
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exp),
		trace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown, nil
}

// InitMeter creates a Prometheus-backed MeterProvider and installs it as the global
// provider (so otel.Meter(...) is backed by it, like InitTracer). Returns a shutdown func.
func InitMeter(serviceName string) (func(context.Context) error, error) {
	exp, err := promexporter.New()
	if err != nil {
		return nil, fmt.Errorf("prometheus exporter: %w", err)
	}

	mp := metric.NewMeterProvider(metric.WithReader(exp))
	otel.SetMeterProvider(mp)
	return mp.Shutdown, nil
}

// MetricsServer returns an *http.Server that serves the default Prometheus
// registry on /metrics. InitMeter registers the OTel→Prometheus exporter there,
// and promauto collectors (e.g. pkg/atrest, pkg/cachemetrics) register there
// too, so a single handler exposes them all. The caller owns Listen/Serve and
// Shutdown — every worker binds it on METRICS_ADDR and stops it during drain.
// Timeouts guard against hung scrapers tying up goroutines.
func MetricsServer() *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}
