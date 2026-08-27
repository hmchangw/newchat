// Package testdata holds the fixtures for metrics.yml.
//
// Those rules are the only enforcement behind the contract's forbidden-label
// list, and before this file existed they were verified by running a probe by
// hand. A pattern edit could therefore have disabled the gate while the
// contract went on claiming it was enforced — the same shape of defect as the
// _INBOX nosemgrep that asserted boundedness nothing checked.
//
// `semgrep scan --test` reads the annotations: a `ruleid:` comment names every
// rule that must fire on the following line, comma-separated. Lines with no
// annotation are negative assertions — a rule firing there is reported as a
// false positive. Both directions matter: a rule that stops flagging is broken,
// and one that starts flagging error_type or reason is broken differently.
//
// Coverage is per independently editable branch, not per rule. Each structural
// pattern and each alternative of the cardinality regex gets its own line, so
// deleting any one of them fails here rather than quietly narrowing the gate.
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

// --- metrics-no-per-call-attribute-set: one line per structural pattern ---

func addShapes(ctx context.Context, c metric.Int64Counter, v string) {
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("site", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(attribute.String("site", v))))
}

func recordShapes(ctx context.Context, h metric.Float64Histogram, seconds float64, v string) {
	// ruleid: metrics-no-per-call-attribute-set
	h.Record(ctx, seconds, metric.WithAttributes(attribute.String("site", v)))
	// ruleid: metrics-no-per-call-attribute-set
	h.Record(ctx, seconds, metric.WithAttributeSet(attribute.NewSet(attribute.String("site", v))))
}

// Observe takes no context, which is why the rule needs patterns of its own.
func observeShapes(o metric.Int64Observer, n int64, v string) {
	// ruleid: metrics-no-per-call-attribute-set
	o.Observe(n, metric.WithAttributes(attribute.String("site", v)))
	// ruleid: metrics-no-per-call-attribute-set
	o.Observe(n, metric.WithAttributeSet(attribute.NewSet(attribute.String("site", v))))
}

// The shape every caller should reach for: the option is built once elsewhere
// and looked up here. No annotation, so any rule firing is a false positive.
func precomputedShapes(
	ctx context.Context,
	c metric.Int64Counter,
	h metric.Float64Histogram,
	o metric.Int64Observer,
	adds map[string]metric.MeasurementOption,
	key string,
	seconds float64,
	n int64,
) {
	c.Add(ctx, 1, adds[key])
	h.Record(ctx, seconds, adds[key])
	o.Observe(n, adds[key])
}

// --- metrics-no-unbounded-label: one line per regex alternative ---

// Every identity root, so deleting any single one from the alternation fails.
func identityRoots(ctx context.Context, c metric.Int64Counter, v string) {
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("room", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("account", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("user", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("message", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("request", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("trace", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("doc", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("span", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("session", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("device", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("tenant", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("org", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("run", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("recipient", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("inbox", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("pod", v)))
}

// Each spelling of the optional id tail.
func identityTails(ctx context.Context, c metric.Int64Counter, v string) {
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("roomID", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("roomId", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("room_id", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("podUID", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("pod_uid", v)))
}

// Subject forms, bare and qualified, plus the Elasticsearch index name.
func subjectAndIndexKeys(ctx context.Context, c metric.Int64Counter, v string) {
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("subject", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("inboxSubject", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("dest_subject", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("index", v)))
	// Qualified forms. The earlier branch was [a-z]*[._]?[sS]ubject, which
	// could only see a single all-lowercase qualifier, so both of these —
	// the shapes a real destination key actually takes — walked past it.
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("destinationInboxSubject", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("messaging.destination.subject", v)))
}

// Raw error text: the bare roots and each message-ish tail.
func rawErrorKeys(ctx context.Context, c metric.Int64Counter, v string) {
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("err", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("error", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("errorText", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("error_message", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("errMsg", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("errorString", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("errStr", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("rawError", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("raw_error", v)))
}

// Stack traces, with and without an error prefix or a trace suffix.
func stackTraceKeys(ctx context.Context, c metric.Int64Counter, v string) {
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("stack", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("stackTrace", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("stack_trace", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("stacktrace", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("errorStack", v)))
}

// The other matching form: a bare attribute.NewSet, not wrapped in an option.
func unboundedInsideNewSet(v string) attribute.Set {
	// ruleid: metrics-no-unbounded-label
	return attribute.NewSet(attribute.String("roomID", v))
}

// The constructor is a metavariable, not String. An int room id is one series
// per room exactly like a string one, so narrowing $CTOR back to String would
// reopen the hole these two lines close.
func nonStringConstructors(ctx context.Context, c metric.Int64Counter, n int64) {
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.Int64("room_id", n)))
	// ruleid: metrics-no-unbounded-label
	_ = attribute.NewSet(attribute.Int("account_id", 3))
}

// attribute.Key("k").String(v) builds the same attribute the other way round.
// Both wrappers need their own pattern, so both get their own line.
func keyBuilderForm(ctx context.Context, c metric.Int64Counter, v string) {
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.Key("room_id").String(v)))
	// ruleid: metrics-no-unbounded-label
	_ = attribute.NewSet(attribute.Key("roomID").String(v))
}

// One line per position where the regex's separator turned from _ into [._],
// because OTel semconv spells all of these dotted.
func dottedSemconvKeys(ctx context.Context, c metric.Int64Counter, v string) {
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("user.id", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("inbox.subject", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("error.text", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("error.stack", v)))
}

// A qualified identity key: the identity root is not the start of the key.
// OTel semconv namespaces its attributes, so the id keys most likely to be
// reached for are spelled this way — messaging.message.id, not message.id —
// and an identity branch anchored at the start of the key saw none of them.
// Each is still one series per entity.
func qualifiedIdentityKeys(ctx context.Context, c metric.Int64Counter, v string) {
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("messaging.message.id", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("messaging_message_id", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("chat.room.id", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("db.session.id", v)))
	// Two qualifier segments, and the bare-root tail: the qualifier is a
	// repeated group, and the id tail stays optional under it.
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("chat.federation.dest.inbox", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.Key("k8s.pod.uid").String(v)))
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
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("errorType", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("error_class", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("result", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("status", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("run_info", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("event_type", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("destination_kind", v)))
}

// The same negative assertion for the forms the rule only just learned to see.
// Widening the constructor and the separator must not start swallowing bounded
// classifications, or the metric loses the one dimension worth slicing on.
func boundedLabelsInWidenedForms(ctx context.Context, c metric.Int64Counter, v string, n int64) {
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("error.type", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("user.agent", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.Key("outcome").String(v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.Int64("recipients", n)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.Bool("run_info", true)))
}

// The negative side of the qualified-identity widening, and the one that
// decides whether it was worth doing. Every key here contains an identity root
// as a namespace segment and is nonetheless bounded, because what follows the
// root is not an id tail. If the qualifier prefix were written loosely enough
// to swallow these, the rule would forbid the entire semconv namespace and get
// switched off — which is a worse outcome than the hole it closes.
func boundedQualifiedKeys(ctx context.Context, c metric.Int64Counter, v string) {
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("http.request.method", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("messaging.destination.name", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("messaging.batch.message_count", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("db.operation.name", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("chat.user.agent", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("k8s.pod.phase", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("chat.error.type", v)))
}

// --- metrics-no-unbounded-label: propagation through a local variable ---

// The structural patterns only saw a constructor nested directly inside a sink.
// Hoisting it into a variable first is not a rewrite anyone does to evade the
// rule — it is what you write when the key is chosen by a branch — but it took
// the attribute out of the pattern's reach all the same. The same shape is why
// search-service's inline fallback option survived a full repo scan.
//
// The rule is taint mode now, so it reports at the sink rather than at the
// constructor. That is the more useful location anyway: the defect is an
// unbounded attribute reaching a metric option, and the sink is where it does.
func unboundedViaLocalVariable(ctx context.Context, c metric.Int64Counter, v string) {
	kv := attribute.String("room_id", v)
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(kv))

	keyed := attribute.Key("accountID").String(v)
	// ruleid: metrics-no-unbounded-label
	_ = attribute.NewSet(keyed)
}

// The same hop with a bounded key must stay silent on the cardinality rule, or
// the dataflow arm has simply widened the gate rather than deepened it.
func boundedViaLocalVariable(ctx context.Context, c metric.Int64Counter, v string) {
	outcome := attribute.String("outcome", v)
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(outcome))

	errType := attribute.Key("error_type").String(v)
	_ = attribute.NewSet(errType)
}

// --- metrics-no-per-call-attribute-set: the option hoisted into a local ---

// The same hop that defeated the cardinality rule defeats this one, and I
// pointed it out myself while fixing search-service's fallback without closing
// it here. An option built one line above the recording call is built per call;
// the variable changes nothing about the cost.
func perCallOptionViaLocal(
	ctx context.Context,
	c metric.Int64Counter,
	h metric.Float64Histogram,
	o metric.Int64Observer,
	seconds float64,
	n int64,
	v string,
) {
	addOpt := metric.WithAttributes(attribute.String("site", v))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, addOpt)

	recOpt := metric.WithAttributeSet(attribute.NewSet(attribute.String("site", v)))
	// ruleid: metrics-no-per-call-attribute-set
	h.Record(ctx, seconds, recOpt)

	obsOpt := metric.WithAttributes(attribute.String("site", v))
	// ruleid: metrics-no-per-call-attribute-set
	o.Observe(n, obsOpt)
}

// Two options in one call. The sink must not depend on the option being the
// last or only argument — Add/Record/Observe are variadic, and a caller passing
// a precomputed set alongside a freshly built one is exactly the mixed case
// where the fresh one still costs per call.
func twoOptionsOneCall(ctx context.Context, c metric.Int64Counter, precomputed metric.MeasurementOption, v string) {
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, precomputed, metric.WithAttributes(attribute.String("site", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("site", v)), precomputed)
}

// The set hoisted a step further out. metric.WithAttributeSet(attribute.NewSet(...))
// was a source only in its nested spelling, so splitting it across two locals
// took the whole flow out of the rule's reach — and attribute.NewSet is the
// expensive half, not the option wrapper around it.
func perCallSetViaTwoLocals(ctx context.Context, c metric.Int64Counter, v string) {
	set := attribute.NewSet(attribute.String("site", v))
	opt := metric.WithAttributeSet(set)
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, opt)
}

// The same set passed straight into the option, one local instead of two.
func perCallSetViaOneLocal(ctx context.Context, c metric.Int64Counter, v string) {
	set := attribute.NewSet(attribute.String("site", v))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributeSet(set))
}

// Camel-qualified identity keys. The qualifier prefix added for
// messaging.message.id required a . or _ separator, so the Go-idiomatic
// spelling of exactly the same key walked past it. destinationRoomID is one
// series per room whatever the separator is — or isn't.
func camelQualifiedIdentityKeys(ctx context.Context, c metric.Int64Counter, v string) {
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("destinationRoomID", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("sourceAccountID", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("parentMessageId", v)))
	// A bare capitalised root with no qualifier at all.
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("RoomID", v)))
	// Both qualifier kinds at once: dotted namespace, then a camel hump.
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("messaging.destinationRoomID", v)))
	// ruleid: metrics-no-per-call-attribute-set, metrics-no-unbounded-label
	c.Add(ctx, 1, metric.WithAttributes(attribute.Key("destinationOrgUID").String(v)))
}

// The camel negatives. A capitalised word that merely contains an identity
// root, or a bounded key whose last segment happens to follow a lowercase run,
// must stay silent — otherwise the camel branch forbids ordinary label names.
func boundedCamelKeys(ctx context.Context, c metric.Int64Counter, v string, n int64) {
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("destinationKind", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("eventType", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("userAgent", v)))
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("runInfo", v)))
	// Plural of a root is a count, not an identity.
	// ruleid: metrics-no-per-call-attribute-set
	c.Add(ctx, 1, metric.WithAttributes(attribute.Int64("recipientCount", n)))
}
