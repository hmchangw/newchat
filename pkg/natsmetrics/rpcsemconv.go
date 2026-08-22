package natsmetrics

import (
	"github.com/flywindy/o11y"
	"go.opentelemetry.io/otel/attribute"
)

// The inbound and outbound request/reply families implement the OpenTelemetry
// RPC semantic conventions instead of carrying names of our own.
//
// The SLO roadmap (docs/load-testing/common/sli-slo.md §8 P1) already asks for
// an RPC server duration histogram sliced by method and error class; the
// convention supplies exactly that, with a spelling every RPC dashboard,
// collector processor and backend already understands. Adopting it is also what
// lets `error.type` be conditional: semconv reserves it for calls that failed,
// so a successful call carries no error label at all — one fewer label value on
// the busiest series in the family, rather than one more.
//
// These names and boundaries are transcribed from
// go.opentelemetry.io/otel/semconv/v1.40.0/rpcconv (instrument names, `s` unit,
// descriptions, `rpc.system.name`) and from the code generator's own boundary
// table in semconv/templates/registry/go/weaver.yaml, which is the normative
// source for the histogram advisory.
//
// They are transcribed here rather than imported from rpcconv because that
// package's Record helpers take the system name as a positional argument and
// append it per call, which allocates on a path we deliberately keep
// allocation-free; see optTable.
//
// The two instrument names are NOT hoisted into constants. The registry guard
// in pkg/obs scans source for instrument-constructor literals, so a name behind
// an identifier is a name it cannot check — see New, where both appear inline.
const (
	// rpcSystemNameKey is the convention's system discriminator. It was
	// `rpc.system` before the RPC stabilization project renamed it.
	rpcSystemNameKey = "rpc.system.name"
	rpcMethodKey     = "rpc.method"
	errorTypeKey     = "error.type"

	// rpcSystemNATS is a custom value: the well-known set is grpc, dubbo,
	// connectrpc and jsonrpc, and the convention allows a custom value for a
	// system outside it.
	rpcSystemNATS = "nats"
)

// rpcDurationBuckets is the one place these families deliberately depart from
// the convention.
//
// The RPC convention prescribes 14 boundaries — o11y's 11 plus 0.075, 0.75 and
// 7.5 — and asks for the identical set for http.server.request.duration. o11y
// already overrides that for every http.server.* histogram, and its reason
// decides this too: "Standardizing these boundaries across the company keeps
// P99 calculations directly comparable between services" (o11y/options.go).
// Taking the convention's set here would leave NATS RPC and HTTP with different
// bucket layouts, so their percentiles could not be compared and no single
// recording rule would fit both.
//
// The deviation is confined to boundaries. Instrument names, the `s` unit,
// rpc.system.name, rpc.method and the conditional error.type all still conform,
// which is where the interoperability actually lives: a generic RPC dashboard
// still finds and groups these series, and only histogram_quantile's
// interpolation points differ.
//
// A latency SLO's bound therefore has to be chosen from this set. SLO-5 was
// drafted at 300 ms, which no set in play has a boundary for; it now reads
// le="0.25" at a 250 ms bound rather than this set gaining a boundary for one
// family. SLO-4's 500 ms already sat on 0.5. See the contract's §7 rule.
var rpcDurationBuckets = o11y.DefaultLatencyBuckets()

// rpcSystemName is constant for the process; the attribute is built once.
var rpcSystemName = attribute.String(rpcSystemNameKey, rpcSystemNATS)

// rpcMethod carries the bounded operation. The convention describes rpc.method
// as the logical method name, which for a subject-routed RPC is the operation
// the subject resolves to — never the subject itself, which carries room and
// account identifiers.
func rpcMethod(operation Operation) attribute.KeyValue {
	return attribute.String(rpcMethodKey, string(operation))
}

func errorType(class string) attribute.KeyValue {
	return attribute.String(errorTypeKey, class)
}
