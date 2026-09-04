# rpc.method Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `rpc_method` vocabulary self-enforcing and stop a telemetry label from killing a process — one declaration list, three cheap gates instead of one expensive one, and the OTel-mandated `_OTHER` fallback.

**Architecture:** The vocabulary currently lives in three places (const block, `normalizeRPCMethod`'s switch, the test file's `allRPCMethods`) with enforcement in the one that is not the guard. Collapse them to a single production-side list that both the lookup and the tests read. Move registration enforcement off the runtime panic and onto two earlier gates — a semgrep rule and a per-service registration test — leaving runtime to log and degrade. Rename the fallback to `_OTHER` per the semconv MUST.

**Tech Stack:** Go 1.25, `pkg/natsmetrics`, `pkg/natsrouter`, semgrep (repo-owned rules), `stretchr/testify`.

**Spec:** This document. Background: `docs/specs/o11y/nats-metrics-contract.md` §13.1 and the branch `e159aa7` it describes.

## Global Constraints

- Go 1.25. Single `go.mod` at repo root. No new third-party dependencies.
- Every label value comes from a closed, code-owned enum. Client-supplied values must never become a metric label.
- RPC method naming: `<verb>_<object>[_qualifier]`, lower `snake_case`, verb first. No dynamic value in a name.
- Commands run through `make`, never raw `go`. `SERVICE=` splices into `./$(SERVICE)/...`, so packages under `pkg/` need the full path: `make test SERVICE=pkg/natsmetrics`. `make lint` is the repo-wide compile gate but does **not** see `//go:build integration` files.
- TDD is mandatory: write the failing test, run it, see it fail, then implement.
- All tests run with `-race`.
- Generated mocks are never hand-edited — `make generate SERVICE=<name>`.
- `make sast-gosec` and `make sast-semgrep-test` must pass. `govulncheck` and semgrep's remote ruleset are blocked by proxy policy in this container; CI owns those.

---

## Why each change

**`_OTHER`, not `unknown`.** `semconv/v1.40.0/attribute_group.go:13902-13934` defines `rpc.method` and states: *"When the method is not recognized … the attribute **MUST** be set to `_OTHER`."* This is normative for `rpc.method` specifically. The earlier reading — that `_OTHER` belongs only to `error.type` and `http.request.method` — came from `rpcconv/metric.go`, which is the metric-helper package and carries no enum; it is not where the attribute is defined.

The same block requires a fully-qualified name (`EchoService/Echo`). We keep short names for Grafana readability, so the contract must record that as a **deliberate deviation**, the way it already records the bucket-boundary deviation — not claim conformance.

**One list, not three.** A method added to the const block and the switch but not to the test file's `allRPCMethods` is fully registerable while violating snake_case, the verb set and the length cap: verified by injecting `MethodFetchRoomConfigurationForTheAdminPanel RPCMethod = "FETCH-RoomConfig"` and watching `make test SERVICE=pkg/natsmetrics` pass. Forget the switch and CI screams; forget the list and CI is silent — and the list is the one carrying the naming rule.

**Log, don't panic.** Enforcement is unconditional but the value is conditional: every service builds its publisher through `NewFromProviderIfEnabled`, so with metrics off `HandledRequest` records nothing and the process still dies for a wrong label. Eight of ten services have no test that runs their registration table, so the panic can first fire in a production pod. Killing a chat service over a telemetry label is the wrong trade — but only once cheaper gates exist, or this is just the silent `unknown` the previous branch removed.

---

## File Structure

**Create**
- `pkg/natsrouter/routes_test.go` — the shared helper each service's registration test calls.
- `<service>/routes_test.go` × 10 — one per service, each building its real router and comparing to a golden file.
- `<service>/testdata/routes.golden` × 10 — generated, never hand-written.
- `.semgrep/rpcmethod.yml` + `.semgrep/rpcmethod.go` — the rule and its fixture.

**Modify**
- `pkg/natsmetrics/rpcmethod.go` — single `rpcMethods` list; `Valid`/`normalizeRPCMethod` become lookups; `MethodUnknown` → `MethodOther`.
- `pkg/natsmetrics/rpcmethod_test.go` — iterate the production list, drop the local copy.
- `pkg/natsmetrics/enums_test.go` — budget counts the production list.
- `pkg/natsmetrics/metrics.go`, `prometheus_export_test.go`, `metrics_test.go`, `toggle_test.go` — `MethodUnknown` → `MethodOther`.
- `pkg/natsmetrics/rpcsemconv.go` — the conformance claim.
- `pkg/natsrouter/router.go` — `addRPCRoute` logs and degrades; export `Routes()`.
- `pkg/natsrouter/router_test.go` — the two panic tests become degradation tests.
- `docs/specs/o11y/nats-metrics-contract.md`, `docs/load-testing/common/sli-slo.md`, `tools/observability/METRICS.md`.

---

### Task 1: One vocabulary list, and `_OTHER`

**Files:** Modify `pkg/natsmetrics/rpcmethod.go`, `rpcmethod_test.go`, `enums_test.go`, `metrics.go`, `metrics_test.go`, `toggle_test.go`, `prometheus_export_test.go`

**Interfaces:**
- Produces: `var rpcMethods = []RPCMethod{…}` (package-level, production file, 92 entries); `const MethodOther RPCMethod = "_OTHER"`; `func (m RPCMethod) Valid() bool`; `func normalizeRPCMethod(m RPCMethod) RPCMethod`. `MethodUnknown` no longer exists.

- [ ] **Step 1: Write the failing tests**

In `rpcmethod_test.go`, delete the local `allRPCMethods` literal and point every guard at the production list. Add the test that proves the guard is now mandatory:

```go
// The naming rule is only a rule if it runs over the same list the lookup
// reads. It did not: the guards iterated a test-file copy while Valid() read a
// switch, so a method added to the switch but not the copy was registerable
// while violating snake_case, the verb set and the length cap.
func TestVocabularyGuardsRunOverTheProductionList(t *testing.T) {
	require.NotEmpty(t, rpcMethods)
	for _, m := range rpcMethods {
		t.Run(string(m), func(t *testing.T) {
			name := string(m)
			assert.Regexp(t, methodFormat, name, "must be lower snake_case")
			assert.LessOrEqual(t, len(name), 40)
			verb, _, ok := cutFirstToken(name)
			require.True(t, ok, "must be <verb>_<object>")
			assert.True(t, verbs[verb], "%q is not an allowed verb", verb)
		})
	}
}

// _OTHER is the semconv-mandated value for an unrecognized method
// (semconv/v1.40.0/attribute_group.go:13902 — "the attribute MUST be set to
// `_OTHER`"). It is the fallback, never a method a route may claim, so it is
// absent from rpcMethods and exempt from the naming rule above.
func TestOtherIsTheFallbackAndNotRegisterable(t *testing.T) {
	assert.Equal(t, RPCMethod("_OTHER"), MethodOther)
	assert.NotContains(t, rpcMethods, MethodOther)
	assert.False(t, MethodOther.Valid())
	assert.Equal(t, MethodOther, normalizeRPCMethod(RPCMethod("not_registered")))
	assert.Equal(t, MethodOther, normalizeRPCMethod(RPCMethod("")))
}

func TestEveryDeclaredMethodIsValidAndNormalizesToItself(t *testing.T) {
	for _, m := range rpcMethods {
		assert.True(t, m.Valid(), "%q must be registerable", m)
		assert.Equal(t, m, normalizeRPCMethod(m))
	}
	assert.Len(t, rpcMethods, 92)
}
```

Keep `methodFormat`, `verbs` and `cutFirstToken` as they are.

- [ ] **Step 2: Run to verify it fails**

Run: `make test SERVICE=pkg/natsmetrics`
Expected: compile failure — `undefined: rpcMethods`, `undefined: MethodOther`.

- [ ] **Step 3: Restructure `rpcmethod.go`**

Keep the const block exactly as it is (the 92 named constants, grouped by owning
service — those are the API). Replace `MethodUnknown` with:

```go
// MethodOther is the value recorded for a method outside the vocabulary.
// semconv makes this normative for rpc.method: "When the method is not
// recognized … the attribute MUST be set to `_OTHER`"
// (semconv/v1.40.0/attribute_group.go:13902).
//
// In steady state it must not appear at all. Registration is gated by a
// semgrep rule and a per-service registration test, so a value reaching here
// means both were bypassed — alert on it rather than treating it as a bucket.
const MethodOther RPCMethod = "_OTHER"
```

Then add the single list and turn the switch into a lookup:

```go
// rpcMethods is the vocabulary. It is the ONLY list: Valid and
// normalizeRPCMethod read it, and the naming guards in rpcmethod_test.go
// iterate it. A method declared above but missing here is not registerable,
// which is a loud failure; the previous shape — a switch here and a separate
// copy in the test file — made the opposite mistake silent.
var rpcMethods = []RPCMethod{
	// room-service
	MethodToggleMute, MethodToggleFavorite, /* … all 92, same order as the const block … */
	MethodGetPresenceSnapshot,
}

var rpcMethodSet = func() map[RPCMethod]struct{} {
	set := make(map[RPCMethod]struct{}, len(rpcMethods))
	for _, m := range rpcMethods {
		set[m] = struct{}{}
	}
	return set
}()

// Valid reports whether m is a declared method. natsrouter calls it at
// registration; a false answer degrades that route to MethodOther rather than
// failing the process.
func (m RPCMethod) Valid() bool {
	_, ok := rpcMethodSet[m]
	return ok
}

func normalizeRPCMethod(m RPCMethod) RPCMethod {
	if m.Valid() {
		return m
	}
	return MethodOther
}
```

Delete the 90-line switch.

- [ ] **Step 4: Replace `MethodUnknown` everywhere**

`grep -rn 'MethodUnknown' --include='*.go' .` and change each to `MethodOther`.
In `enums_test.go` the budget asserts must count `len(rpcMethods)+1` (the +1 is
`MethodOther`), keeping 744 and 837.

- [ ] **Step 5: Run to verify it passes**

Run: `make test SERVICE=pkg/natsmetrics && make lint`
Expected: PASS, 0 issues.

- [ ] **Step 6: Commit**

```bash
git add pkg/natsmetrics
git commit -m "refactor(o11y): one vocabulary list, and _OTHER as the semconv fallback"
```

---

### Task 2: Registration degrades instead of panicking, and exposes its table

**Files:** Modify `pkg/natsrouter/router.go`, `pkg/natsrouter/router_test.go`

**Interfaces:**
- Consumes: `MethodOther`, `Valid()` from Task 1.
- Produces: `func (r *Router) Routes() map[natsmetrics.RPCMethod]string` — a copy, safe to hold.

- [ ] **Step 1: Rewrite the two panic tests as degradation tests**

Replace `TestRegisterPanicsOnUndeclaredMethod` and
`TestRegisterPanicsOnDuplicateMethodInOneRouter` (`router_test.go:955,970`):

```go
// An undeclared method must not kill the process. Registration is gated
// earlier — by .semgrep/rpcmethod.yml and by each service's registration test
// — so a value arriving here means both were bypassed. Degrade to _OTHER,
// which is alertable, and keep serving: a chat service is worth more than a
// clean dashboard.
func TestRegisterDegradesUndeclaredMethodToOther(t *testing.T) {
	nc := startTestNATS(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	r := New(nc, "test", WithMetrics(natsmetrics.NewFromProvider(mp).Publisher("site-a")))

	require.NotPanics(t, func() {
		Register(r, "chat.user.{account}.request.room.{roomID}.site-a.open",
			natsmetrics.RPCMethod("not_registered"),
			func(_ *Context, req testReq) (*testResp, error) { return &testResp{Greeting: "ok"}, nil })
	})

	_, err := nc.Request(context.Background(),
		"chat.user.alice.request.room.room-a.site-a.open", []byte(`{"name":"ok"}`), 2*time.Second)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return assert.ObjectsAreEqual([]string{"_OTHER"}, serverCallMethods(t, reader))
	}, time.Second, 10*time.Millisecond)
}

// A duplicate method merges two routes into one series — a blunt signal, but
// refusing the registration would drop the API entirely, which is an outage.
// Register both and let the per-service golden test be the gate.
func TestRegisterKeepsBothRoutesOnDuplicateMethod(t *testing.T) {
	r := New(startTestNATS(t), "room-service")
	handler := func(_ *Context, req testReq) (*testResp, error) { return &testResp{Greeting: "ok"}, nil }

	require.NotPanics(t, func() {
		Register(r, "chat.user.{account}.request.room.{roomID}.site-a.open",
			natsmetrics.MethodOpenRoom, handler)
		Register(r, "chat.user.{account}.request.room.{roomID}.site-a.app.tabs",
			natsmetrics.MethodOpenRoom, handler)
	})

	// Both subscriptions live; the map keeps the last claimant.
	assert.Len(t, r.Routes(), 1)
}

// Routes is what each service's registration test compares to its golden file,
// so it must be a copy — a caller holding the router's own map could mutate
// the dispatch table.
func TestRoutesReturnsACopy(t *testing.T) {
	r := New(startTestNATS(t), "test")
	Register(r, "chat.user.{account}.request.room.{roomID}.site-a.open",
		natsmetrics.MethodOpenRoom,
		func(_ *Context, req testReq) (*testResp, error) { return &testResp{}, nil })

	got := r.Routes()
	require.Len(t, got, 1)
	delete(got, natsmetrics.MethodOpenRoom)
	assert.Len(t, r.Routes(), 1, "Routes must not hand out the router's own map")
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `make test SERVICE=pkg/natsrouter`
Expected: FAIL — the registrations panic, and `Routes` is undefined.

- [ ] **Step 3: Rewrite `addRPCRoute`**

```go
// addRPCRoute registers a reply-bearing route. Both checks log and degrade
// rather than panic: every production call site passes a compile-time
// constant, so a bad method is a code defect caught by .semgrep/rpcmethod.yml
// and by the service's registration test — both cheaper than a crash loop.
// Metrics are opt-in (NewFromProviderIfEnabled), so a panic here would kill a
// process over a value it may not even record.
func (r *Router) addRPCRoute(pattern string, method natsmetrics.RPCMethod, handlers []HandlerFunc) {
	if !method.Valid() {
		slog.Error("natsrouter: route declares an rpc method outside the vocabulary; its samples record as _OTHER",
			"pattern", pattern, "method", method)
		method = natsmetrics.MethodOther
	}
	r.mu.Lock()
	if claimed, dup := r.methods[method]; dup {
		// Registering anyway: refusing would drop the route, and a merged
		// series is a blunt signal where a missing API is an outage.
		slog.Error("natsrouter: rpc method already claimed; these routes' samples merge into one series",
			"method", method, "claimed_by", claimed, "pattern", pattern)
	}
	r.methods[method] = pattern
	r.mu.Unlock()

	r.addRoute(pattern, method, true, handlers)
}

// Routes returns the method-to-pattern table this router registered, copied so
// a caller cannot reach the dispatch state. Each service's registration test
// compares it to a golden file, which is what pins a route to its correct
// method — the duplicate check above only catches collisions.
func (r *Router) Routes() map[natsmetrics.RPCMethod]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[natsmetrics.RPCMethod]string, len(r.methods))
	for m, p := range r.methods {
		out[m] = p
	}
	return out
}
```

Initialise `r.methods` in `New` so the nil guard can go.

- [ ] **Step 4: Run to verify they pass**

Run: `make test SERVICE=pkg/natsrouter && make lint`
Expected: PASS, 0 issues.

- [ ] **Step 5: Commit**

```bash
git add pkg/natsrouter
git commit -m "feat(natsrouter): degrade a bad rpc method instead of panicking; expose Routes"
```

---

### Task 3: A registration golden test per service

**Files:** Create `<service>/routes_test.go` and `<service>/testdata/routes.golden` for all ten services.

**Interfaces:** Consumes `Router.Routes()` from Task 2.

This is the gate that replaces the panic, and it closes the coverage hole: only
room-service and translation-service currently have any test that runs their
real registration table.

- [ ] **Step 1: Write one service's test first (room-service), and see it fail**

```go
package main

import (
	"flag"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/routes.golden")

// The router's own duplicate check catches collisions, not a valid-but-wrong
// constant on a route. This golden file is what pins each route to its method:
// a copy-pasted wrong constant shows up in review as a one-line diff.
// Regenerate with: make test SERVICE=room-service ARGS="-run TestRegisteredRoutes -update"
func TestRegisteredRoutes(t *testing.T) {
	r := newTestRouter(t)      // whatever this service's tests already use
	newHandler(t).Register(r)  // the service's real registration entry point

	var lines []string
	for method, pattern := range r.Routes() {
		lines = append(lines, string(method)+" "+pattern)
	}
	sort.Strings(lines)
	got := strings.Join(lines, "\n") + "\n"

	path := filepath.Join("testdata", "routes.golden")
	if *updateGolden {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(path, []byte(got), 0o644))
		return
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err, "run with -update to create the golden file")
	require.Equal(t, string(want), got)
}
```

Adapt the two setup lines to each service — read how that service's existing
tests build a router and call its registration function. Where a service has no
such helper, build the router with `natsrouter.New` over `startTestNATS`'s
equivalent; `pkg/natsrouter`'s `startTestNATS` runs an embedded server
in-process, so no Docker is involved and this stays a `make test` unit test.

- [ ] **Step 2: Run to verify it fails**

Run: `make test SERVICE=room-service`
Expected: FAIL — golden file missing.

- [ ] **Step 3: Generate the golden file and confirm it passes**

Regenerate, then read the file and check every line against
`docs/superpowers/plans/2026-09-03-rpc-method-registration-vocabulary.md`'s
naming table. **A wrong constant here is exactly what this test exists to
catch — do not accept a generated file without reading it.**

- [ ] **Step 4: Repeat for the other nine**

`history-service`, `user-service`, `search-service`, `media-service`,
`translation-service`, `user-presence-service`, `bot-room-service`,
`bot-message-handler`, `room-worker`. `user-presence-service`'s golden holds
three entries, not seven — its four `RegisterVoid` routes carry no method.

- [ ] **Step 5: Verify**

Run: `make test` and `make lint`
Expected: no FAIL, 0 issues.

- [ ] **Step 6: Commit**

```bash
git add .
git commit -m "test(o11y): pin every service's route-to-method table with a golden file"
```

---

### Task 4: A semgrep rule for the registration argument

**Files:** Create `.semgrep/rpcmethod.yml`, `.semgrep/rpcmethod.go`

The type system stops a transposed argument, but an untyped string literal is
assignable to `RPCMethod`, so `Register(r, pattern, "rename_room", fn)`
compiles. This is the cheapest gate and it catches that one hole.

- [ ] **Step 1: Write the fixture first**

`.semgrep/rpcmethod.go` — annotate the lines the rule must flag with
`// ruleid: rpcmethod-must-be-a-vocabulary-constant`, and leave the correct
forms unannotated. Cover: a string literal in the method position (flagged); a
`natsmetrics.MethodX` selector (not flagged); `RegisterVoid`, which takes no
method (not flagged).

- [ ] **Step 2: Write the rule**

`.semgrep/rpcmethod.yml`, matching the style of `.semgrep/metrics.yml`. The
message must say what to do, not just what is wrong: pass a `natsmetrics.Method*`
constant, and add one to `pkg/natsmetrics/rpcmethod.go` if the route needs a
new method.

- [ ] **Step 3: Run the rule's own tests**

Run: `make sast-semgrep-test`
Expected: the new rule's fixture passes alongside `metrics.yml`'s.

- [ ] **Step 4: Confirm the rule finds nothing in the tree**

Run: `make sast-gosec` and the repo-owned semgrep scan over `pkg/` and the ten
services.
Expected: clean — every production call site already passes a constant.

- [ ] **Step 5: Commit**

```bash
git add .semgrep
git commit -m "build(sast): require a vocabulary constant in the rpc method argument"
```

---

### Task 5: Documentation

**Files:** Modify `pkg/natsmetrics/rpcsemconv.go`, `docs/specs/o11y/nats-metrics-contract.md`, `docs/load-testing/common/sli-slo.md`, `tools/observability/METRICS.md`

- [ ] **Step 1: Correct the semconv conformance claim**

`rpcsemconv.go`'s comment currently says instrument names, the unit,
`rpc.system.name`, `rpc.method` and the conditional `error.type` "all still
conform". `rpc.method` does not: semconv asks for a fully-qualified name
(`EchoService/Echo`) and these are short (`rename_room`). Record it as a
deliberate deviation with its reason — the file already has exactly this shape
for the bucket-boundary deviation, so match it. State that `service_name`
supplies the qualifier a fully-qualified name would carry, and that the
`_OTHER` fallback and the conditional `error.type` do conform.

- [ ] **Step 2: Fix the count in two places**

`nats-metrics-contract.md` and `sli-slo.md` say "92 constants for 91 routes plus
one client-only method". There are **92** routes, not 91. Correct: 91 method
names cover 92 routes (`mark_all_threads_read` is registered by both
room-service and user-service), plus one client-only method
(`get_presence_snapshot`), giving 92 constants.

- [ ] **Step 3: Rewrite the ops runbook entry**

`tools/observability/METRICS.md:89-90` still says "services whose subjects have
no operation mapping yet record `rpc_method="unknown"`" — the deleted mechanism,
stated as current behaviour, in the file an on-call engineer reads. Replace with
the real one: the method is declared at route registration; `RegisterVoid`
routes record no RPC sample at all; and `rpc_method="_OTHER"` is not a fallback
bucket but a bug signal that should be zero, worth an alert.

- [ ] **Step 4: Update §13.1 of the contract**

Describe the three gates (semgrep, the per-service golden test, the runtime
degradation) and say plainly that the runtime no longer panics and why. Keep
the existing sentence about nothing pinning a route to its correct method, but
point it at the golden files, which now do.

- [ ] **Step 5: Verify**

Run: `grep -rn 'unknown' tools/observability/METRICS.md docs/specs/o11y/nats-metrics-contract.md`
Expected: no surviving claim that `unknown` is a normal `rpc_method` value.

Run: `make lint`
Expected: 0 issues.

- [ ] **Step 6: Commit**

```bash
git add pkg/natsmetrics/rpcsemconv.go docs tools/observability/METRICS.md
git commit -m "docs(o11y): _OTHER, the three gates, and the fully-qualified deviation"
```

---

## Out of Scope

- **An instrument for the `RegisterVoid` lane.** The presence heartbeat still emits no server-side latency signal. The justification in `register.go` — that recording it would pull every percentile down — was true only while every route shared one label, and this branch's own change invalidated it. It needs a new instrument (`chat.nats.handler.duration`) and its own contract entry, so it gets its own PR.
- **The `le=10` bucket ceiling**, which is why a p99 "pinned at 10s" cannot be distinguished from 60s. Shared with `http.server.*` by an explicit o11y decision; changing it is a fleet-wide conversation.
- **The nine uninstrumented outbound RPC call sites** in `user-service`, `search-service` and `notification-worker`.
- **Verb synonym drift** (`remove_member` vs `delete_emoji`, `batch_get_rooms` vs `list_members`). Worth a shrink of the verb set and a picking rule in the contract, but it is a vocabulary-wide edit, not part of making the guard mechanical.
