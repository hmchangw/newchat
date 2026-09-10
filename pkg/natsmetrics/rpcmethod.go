package natsmetrics

// RPCMethod is the rpc.method label on rpc_server_call_duration_seconds and its
// client twin. It is a closed vocabulary, declared at route registration rather
// than derived from the NATS subject: the old subject parser only recognised the
// room and orgs families, so 51 of 96 routes recorded as "unknown" — every
// user-service, search, presence, translation, media and bot route among them.
//
// Names are verb-first snake_case, and which verb is right is a semantic rule no
// test can check, so it is written down here. Following AIP-131/132/190:
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
//
// One value is deliberately shared across two services: mark_all_threads_read is
// both user-service's client-facing route and the room-service route user-service
// calls to serve it. They are one logical operation over two hops, and a panel
// filtered on the method shows both.
type RPCMethod string

//go:generate go run github.com/hmchangw/chat/tools/rpcmethodgen -in rpcmethods.tsv -out rpcmethod_gen.go

// MethodOther is the record-time fallback for a method outside the vocabulary.
// semconv v1.40.0 makes "_OTHER" normative for an unrecognised rpc.method, and
// addRPCRoute degrades to it rather than panicking: a telemetry label defect
// should not stop a process, and metrics are opt-in, so a panic could kill a
// service over a value it might not even record. It is deliberately not Valid(),
// so no route can claim it and the fallback keeps meaning "should never happen".
const MethodOther RPCMethod = "_OTHER"

// MethodNone marks a route that records no rpc.server.call.duration sample.
// RegisterVoid routes carry it: a void handler sends no reply, so there is no
// round trip to time, and recording local handler cost under a call-duration
// histogram would misreport what the metric means. It is the zero RPCMethod so
// the intent is explicit at the call site rather than an omitted argument.
const MethodNone RPCMethod = ""

// rpcMethodVocabulary is the membership set Valid() reads, derived from the
// generated slice rather than written out a second time. A map rather than a
// linear scan because Valid() is on the registration path for every route.
var rpcMethodVocabulary = func() map[RPCMethod]struct{} {
	set := make(map[RPCMethod]struct{}, len(rpcMethods))
	for _, m := range rpcMethods {
		set[m] = struct{}{}
	}
	return set
}()

// Valid reports whether m is one of the vocabulary constants. MethodOther is
// not, by design.
func (m RPCMethod) Valid() bool {
	_, ok := rpcMethodVocabulary[m]
	return ok
}
