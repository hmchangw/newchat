package natsmetrics

import "regexp"

// RPCMethod is the rpc.method label on rpc_server_call_duration_seconds and its
// client twin. It is declared by the route table that registers the handler
// (natsrouter.Route), not derived from the NATS subject: the old subject parser
// only recognised the room and orgs families, so 51 of 96 routes recorded as
// "unknown" — every user-service, search, presence, translation, media and bot
// route among them.
//
// There is deliberately no const vocabulary here. The set of methods the fleet
// uses is exactly the set its route tables declare, so a closed list in this
// package would be a second copy of that set, kept in step by hand. What remains
// is the naming rule, which no list could check anyway.
//
// Names are verb-first snake_case, following AIP-131/132/190:
//
//	get_        one logical resource
//	list_       a collection, whether or not it carries a total
//	batch_get_  several specific resources by caller-supplied keys
//	search_     a query
//
// A method names what the handler does, not what its subject happens to spell.
// Where the two disagree the handler wins — mark_room_read advances a room read
// position and carries no message id anywhere on its path, despite living under
// a .message. subject.
type RPCMethod string

// MethodOther is the record-time fallback for a method that never passed a route
// table's validation. semconv v1.40.0 makes "_OTHER" normative for an
// unrecognised rpc.method, and the record site degrades to it rather than
// panicking: a telemetry label defect should not stop a process. It is
// deliberately not IsValidRPCMethod, so no route can claim it and the fallback
// keeps meaning "should never happen".
const MethodOther RPCMethod = "_OTHER"

// MethodNone marks a route that records no rpc.server.call.duration sample.
// HandleVoid routes carry it: a void handler sends no reply, so there is no
// round trip to time, and recording local handler cost under a call-duration
// histogram would misreport what the metric means. It is the zero RPCMethod, so
// a void row in a route table simply omits the field.
const MethodNone RPCMethod = ""

// rpcMethodShape is the naming rule, and the only gate a method passes.
// natsrouter.ValidateRoutes applies it at startup so a bad name fails the
// service rather than the dashboard; normalizeRPCMethod applies it again at the
// record site for a value that reached the metric without passing a table.
var rpcMethodShape = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)

// IsValidRPCMethod reports whether method is a usable rpc.method label.
// MethodNone and MethodOther are both invalid, by design.
func IsValidRPCMethod(method RPCMethod) bool {
	return rpcMethodShape.MatchString(string(method))
}

// normalizeRPCMethod bounds the rpc.method label. An unusable method — including
// the zero value, only reachable by a caller bypassing a route table — records as
// MethodOther rather than minting a series from an unbounded value, or vanishing.
func normalizeRPCMethod(method RPCMethod) RPCMethod {
	if IsValidRPCMethod(method) {
		return method
	}
	return MethodOther
}
