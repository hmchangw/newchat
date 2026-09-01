package natsmetrics

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// newPrometheusExportSetup mirrors the o11y SDK's Prometheus pull path: the
// allow-listed resource attributes are rendered as constant labels on every
// series. A metric attribute whose sanitized name matches one of them makes
// reg.Gather() fail with a duplicate-label error, which promhttp surfaces as
// an HTTP 500 on /metrics — taking every chat_nats_* family down with it.
func newPrometheusExportSetup(t *testing.T) (*Metrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	exporter, err := otelprom.New(
		otelprom.WithRegisterer(reg),
		otelprom.WithResourceAsConstantLabels(attribute.NewAllowKeysFilter(
			"service.namespace", "service.name", "service.version", "deployment.environment.name",
		)),
	)
	require.NoError(t, err)
	res := resource.NewSchemaless(
		attribute.String("service.namespace", "chat"),
		attribute.String("service.name", "broadcast-worker"),
		attribute.String("service.version", "dev"),
		attribute.String("deployment.environment.name", "development"),
		attribute.String("site.id", "s1"),
	)
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter), sdkmetric.WithResource(res))
	t.Cleanup(func() { require.NoError(t, mp.Shutdown(context.Background())) })
	return NewFromProvider(mp), reg
}

func TestPrometheusExport_GatherSucceedsWithResourceConstantLabels(t *testing.T) {
	m, reg := newPrometheusExportSetup(t)
	ctx := context.Background()

	c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "MESSAGES_CANONICAL_s1", Consumer: "broadcast-worker"})
	c.LoopStarted(ctx)
	msg := &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}
	require.NoError(t, c.Track(ctx, msg, EventCreated, 5).Ack())

	p := m.Publisher("s1")
	// A real failure, not nil: Failure returns before recording on a nil error,
	// so the nil call left chat_nats_publish_failures out of the exposition
	// entirely and this guard silently stopped covering it.
	p.Failure(ctx, DestinationCanonical, OperationCanonicalPublish, nats.ErrTimeout)
	p.Request(ctx, OperationPresenceLookup, time.Millisecond, nil)
	p.HandledRequest(ctx, OperationRoomRead, time.Millisecond, RequestSuccess)

	families, err := reg.Gather()
	require.NoError(t, err, "gather must succeed — a failure here is the /metrics 500")

	// The RPC families do not carry the chat_nats_ prefix — that is the point of
	// adopting the semantic convention — so a prefix filter would have excluded
	// the two newest families from the guard that exists to catch this.
	covered := map[string]bool{}
	for _, mf := range families {
		name := mf.GetName()
		if !strings.HasPrefix(name, "chat_nats_") && !strings.HasPrefix(name, "rpc_") {
			continue
		}
		covered[name] = true
		for _, metric := range mf.GetMetric() {
			values := map[string][]string{}
			for _, lp := range metric.GetLabel() {
				values[lp.GetName()] = append(values[lp.GetName()], lp.GetValue())
			}
			for name, vals := range values {
				assert.Lenf(t, vals, 1, "family %s repeats label %s (values %v)", mf.GetName(), name, vals)
			}
			assert.Equalf(t, []string{"broadcast-worker"}, values["service_name"],
				"family %s must carry the resource-derived service_name exactly once", mf.GetName())
		}
	}

	// Naming the families explicitly, so a family that stops being exported
	// fails here instead of quietly leaving the guard with nothing to check.
	for _, family := range []string{
		"chat_nats_consumer_loop_up",
		"chat_nats_consumer_messages_total",
		"chat_nats_publish_failures_total",
		"rpc_client_call_duration_seconds",
		"rpc_server_call_duration_seconds",
	} {
		assert.Truef(t, covered[family], "%s must be in the exposition for this guard to mean anything", family)
	}
}
