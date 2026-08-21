package natsmetrics

import "go.opentelemetry.io/otel/attribute"

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

// rpcDurationBuckets are the convention's boundaries. An RPC panel that assumes
// them reads the wrong quantile off our generic latency buckets.
var rpcDurationBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
}

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
