// Package testdata holds the fixtures for metrics.yml.
//
// Those rules are the only enforcement behind the contract's forbidden-label
// list, and until this file existed they were verified by running a probe by
// hand. A regex edit could therefore have disabled the gate while the contract
// went on claiming it was enforced — the same shape of defect as the _INBOX
// nosemgrep that asserted boundedness nothing checked.
//
// `semgrep scan --test` reads the annotations below: a `ruleid:` comment names
// every rule that must fire on the following line, comma-separated. Lines with
// no annotation are negative assertions — a rule firing there is reported as a
// false positive — which is why the bounded labels at the bottom carry no
// comment. Both directions matter: a rule that stops flagging is broken, and
// one that starts flagging error_type or reason is broken differently.
//
// The file sits beside metrics.yml because the test runner matches a rule file
// to a target of the same basename and does not support a separate tests
// directory. The Go toolchain ignores any directory beginning with a dot, so it
// never reaches go build or golangci-lint, and SEMGREP_FLAGS excludes .semgrep
// from the scanning run that would otherwise report these deliberate
// violations as real findings.
package testdata

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Identity keys, with and without an id tail.
func identityKeys(ctx context.Context, c metric.Int64Counter, v string) {
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("roomID", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("room", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("run_id", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("recipient", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("podUID", v)))
}

// Subject-shaped keys, bare and qualified.
func subjectKeys(ctx context.Context, c metric.Int64Counter, v string) {
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("subject", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("inboxSubject", v)))
}

// Raw error text and stack traces.
func errorKeys(ctx context.Context, c metric.Int64Counter, v string) {
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("error", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("error_message", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("stackTrace", v)))
}

// Bounded labels, including the error *classifications* the contract allows.
// The per-call rule still fires on each — that is the separate complaint about
// building the set inline — but the cardinality rule must not, and the absence
// of its name in these annotations is what asserts that. A rule that swallowed
// error_type or reason would push real classification onto the log line and
// leave the metric useless.
func boundedLabels(ctx context.Context, c metric.Int64Counter, v string) {
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("site", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("error_type", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("run_info", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("event_type", v)))
}

// A precomputed option looked up by a bounded key is the shape both rules exist
// to steer callers towards, so neither may fire on it. No annotation is the
// assertion.
func precomputed(ctx context.Context, c metric.Int64Counter, opts map[string]metric.MeasurementOption, key string) {
	c.Add(ctx, 1, opts[key])
}
