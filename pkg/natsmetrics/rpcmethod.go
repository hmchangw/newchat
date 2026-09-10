package natsmetrics

import "strings"

// RPCMethod is the rpc.method label on rpc_server_call_duration_seconds.
//
// It is derived once, at route registration, from the subject pattern the route
// registers — not matched at dispatch time and not declared by hand. The
// distinction matters: RequestOperationFromSubject pattern-matched suffixes on
// every message and gated them on the subject's family token, so its default arm
// swallowed 51 of the fleet's 96 routes into "unknown". Deriving mechanically
// from the whole pattern has no default arm, so a route cannot go unnamed.
//
// The subject is the client wire contract (docs/client-api.md), which makes it a
// far more stable source than a Go identifier: renaming one is already a
// breaking change that CLAUDE.md requires be documented in the same PR, so a
// metric rename can never sneak in behind a local refactor.
//
// The cost is that names read as the wire spells them rather than as the handler
// behaves — room_message_read, not mark_room_read.
type RPCMethod string

// MethodOther is the fallback for a pattern that derives no usable name.
// semconv v1.40.0 makes "_OTHER" normative for an unrecognised rpc.method.
// Derivation degrades to it rather than panicking: a telemetry defect must not
// stop a process, and a bounded wrong value stays alertable where an absent one
// does not.
const MethodOther RPCMethod = "_OTHER"

// structuralTokens carry transport shape rather than meaning, so they are
// dropped wherever they appear. "user" is deliberately absent: it is structural
// only at index 1, where it names the client transport lane, and meaningful
// anywhere else — chat.user.{account}.request.user.{site}.me needs the second
// one, and teams.call.user ends in it.
var structuralTokens = map[string]struct{}{
	"chat":     {},
	"request":  {},
	"event":    {},
	"internal": {},
}

// MethodFromPattern derives the rpc.method label from a registered subject
// pattern. siteID is the service's own site, which most patterns interpolate as
// a literal token and the rest carry as a {siteID} placeholder.
//
// Placeholders are dropped because a room or account id in a label is unbounded
// cardinality. Everything that survives is a fixed token from the pattern, so
// the label set is bounded by the number of registered routes.
//
// Only the client lane token ("user" at index 1) is dropped. Every other lane
// marker — "server", "bot", "migration" — is kept, because dropping them
// collides real routes: without "bot", bot-room-service's room.create derives
// the same name as room-service's, and without "server",
// chat.server.request.presence.{site}.query.batch derives the same name as
// chat.user.presence.{site}.query.batch. A collision is exactly what makes
// rpc_method unusable as an aggregation key.
func MethodFromPattern(pattern, siteID string) RPCMethod {
	tokens := strings.Split(pattern, ".")
	parts := make([]string, 0, len(tokens))

	// Only the first siteID-valued token is dropped, so a service whose site
	// happens to be spelled like a family token keeps the second occurrence.
	siteDropped := false

	for i, token := range tokens {
		switch {
		case token == "":
			continue
		case isPlaceholder(token):
			continue
		case !siteDropped && token == siteID:
			siteDropped = true
			continue
		case isStructural(token):
			continue
		case i == 1 && token == "user":
			continue
		}
		parts = append(parts, normalizeToken(token))
	}

	if len(parts) == 0 {
		return MethodOther
	}

	method := strings.Join(parts, "_")
	if !isLabelShaped(method) {
		return MethodOther
	}

	return RPCMethod(method)
}

func isPlaceholder(token string) bool {
	return len(token) > 2 && token[0] == '{' && token[len(token)-1] == '}'
}

func isStructural(token string) bool {
	_, ok := structuralTokens[token]
	return ok
}

// normalizeToken lowers a subject token into label spelling: subjects carry both
// camelCase (subscription.getByRoomID) and hyphens (message.read-receipt), and
// neither reads as a metric label.
func normalizeToken(token string) string {
	var b strings.Builder
	b.Grow(len(token) + 4)

	for i := 0; i < len(token); i++ {
		c := token[i]
		switch {
		case c == '-':
			b.WriteByte('_')
		case c >= 'A' && c <= 'Z':
			if i > 0 && breaksWord(token, i) {
				b.WriteByte('_')
			}
			b.WriteByte(c - 'A' + 'a')
		default:
			b.WriteByte(c)
		}
	}

	return b.String()
}

// breaksWord reports whether the upper-case byte at i starts a new word.
// A run of capitals is one word so a trailing initialism survives intact:
// getByRoomID splits to get_by_room_id, not get_by_room_i_d.
func breaksWord(token string, i int) bool {
	prev := token[i-1]
	if isLower(prev) || isDigit(prev) {
		return true
	}
	return isUpper(prev) && i+1 < len(token) && isLower(token[i+1])
}

func isLower(c byte) bool { return c >= 'a' && c <= 'z' }
func isUpper(c byte) bool { return c >= 'A' && c <= 'Z' }
func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// isLabelShaped is the backstop: anything a future subject token could carry
// that would not read as a label value collapses to MethodOther rather than
// reaching Prometheus.
func isLabelShaped(method string) bool {
	if method == "" || method[0] == '_' || method[len(method)-1] == '_' {
		return false
	}
	for i := 0; i < len(method); i++ {
		c := method[i]
		if isLower(c) || isDigit(c) {
			continue
		}
		if c == '_' && method[i-1] != '_' {
			continue
		}
		return false
	}
	return true
}

// normalizeRPCMethod is the record-site backstop for a method that did not come
// from MethodFromPattern — a caller passing a zero value, or a future derivation
// change. Bounding here keeps a telemetry defect from becoming unbounded label
// cardinality, which is the one failure that damages the metrics backend rather
// than just the panel reading it.
func normalizeRPCMethod(method RPCMethod) RPCMethod {
	if method == MethodOther || !isLabelShaped(string(method)) {
		return MethodOther
	}
	return method
}
