# Priority-Contact Presence Gating Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the priority-contact pierce cross the presence gate, and wire the issue's do-not-disturb / presenting rule into the gate as inert predicates for the presence side to fill in.

**Architecture:** All behaviour lives in one pure function, `shouldPush` in `notification-worker/presence.go`. Two new predicates, `isDND` and `isPresenting`, are declared as package-level function vars returning `false` — deriving those statuses belongs to the presence side of the system, and this worker must not infer them from `busy`/`in-call`. `isInCall` is left byte-identical to what shipped. Nothing else in the pipeline changes: the candidate loop, the settings/presence snapshots, survivor sorting, batching and dedup are untouched.

**Tech Stack:** Go 1.25, `stretchr/testify` for assertions, table-driven tests, `make` targets.

**Spec:** `docs/superpowers/specs/2026-08-10-priority-contact-presence-gating-design.md`

## Global Constraints

- TDD is mandatory (CLAUDE.md §4): write the tests, run them, **confirm they fail**, then implement. Task 1 is ordered so every test exists before any implementation.
- **Never infer DND or presenting from a status we already receive.** No mapping DND→`busy`, no mapping presenting→`in-call`. Those statuses are owned by the presence side; this worker declares the predicates and gates on them.
- **Do not narrow `isInCall`.** It must keep matching both `busy` and `in-call` while `isDND` is inert, or every do-not-disturb user starts receiving pushes during the gap.
- Use `make` targets, not raw `go` commands — except for coverage, where CLAUDE.md itself prescribes `go test -coverprofile` / `go tool cover -func`.
- Unit tests for this service: `make test SERVICE=notification-worker` (expands to `go test -race ./notification-worker/...`). The target does **not** forward `-run`; to run a single test use `go test -race ./notification-worker/ -run '<Name>' -v` directly, and finish with the `make` target.
- Test files stay in `package main` so they can reach unexported identifiers.
- Tests that swap the stub vars MUST restore them via `t.Cleanup` — CLAUDE.md forbids shared mutable state between tests. Do not add `t.Parallel()` to those tests.
- Comments follow the repo's WHY-not-WHAT convention: max 2 lines, rationale only.
- Minimum 80% coverage; `shouldPush` and `isInCall` must reach 100%.
- Lint and tests are enforced by a pre-commit hook. A failing hook means fix, not retry.
- Do NOT add env vars, model fields, RPCs or events. `USER_SETTINGS_ENABLED=false` is already the kill switch.
- Do NOT modify `handler.go`'s candidate loop. A per-room-muted member must keep being dropped before the gate — no pierce resurrects them.

---

### Task 1: Wire rule 2 as inert predicates and make the pierce total

Every test lands before the implementation, so the red phase is real.

**Files:**
- Modify: `notification-worker/presence.go` (add stub vars; rewrite `shouldPush`; leave `isInCall` alone)
- Modify: `notification-worker/handler.go:200-207` (comment block above the two `Snapshot` calls)
- Test: `notification-worker/presence_test.go` (`TestShouldPush`, plus two helpers and a stub-contract test)
- Test: `notification-worker/handler_test.go` (append two tests)

**Interfaces:**
- Consumes: `notifSettings` from `usersettings.go` — fields `muteAll`, `allowPriority`, `showInCall bool`, `priorityContacts map[string]struct{}`. `model.Presence{AggregatedStatus string}`. Existing fixtures in `handler_test.go`: `newTestHandlerWithSettings(members, presence, settings, hook, emit)`, `stubMembers{out map[string][]roomsubcache.Member}`, `stubPresence{out map[string]model.Presence}`, `stubSettings{out map[string]notifSettings, err error}`, `recordingEmitter` with `.accounts() []string`, `msgEvent(*model.Message) []byte`, `noopVetoer{}`.
- Produces: `var isDND, isPresenting func(model.Presence) bool`; `shouldPush(p model.Presence, ns notifSettings, isPrioritySender bool) bool` — **signature unchanged**, so `handler.go:213`'s call site keeps compiling untouched. Test helpers `stubPresenceFlags(t, dnd, presenting bool)` and `stubPresenceFlagsByStatus(t, dndStatus, presentingStatus string)`.

- [ ] **Step 1: Add the two test helpers**

Insert above `TestShouldPush` in `presence_test.go`:

```go
// stubPresenceFlags swaps the isDND/isPresenting stubs for one test and restores
// them afterwards, so a row that flips them cannot leak into a sibling test.
func stubPresenceFlags(t *testing.T, dnd, presenting bool) {
	t.Helper()
	origDND, origPresenting := isDND, isPresenting
	isDND = func(model.Presence) bool { return dnd }
	isPresenting = func(model.Presence) bool { return presenting }
	t.Cleanup(func() { isDND, isPresenting = origDND, origPresenting })
}

// stubPresenceFlagsByStatus points the isDND/isPresenting stubs at one status
// each, restoring them afterwards. Lets a handler test prove the gate consults
// both without asserting any real status mapping; "" disables that stub.
func stubPresenceFlagsByStatus(t *testing.T, dndStatus, presentingStatus string) {
	t.Helper()
	origDND, origPresenting := isDND, isPresenting
	isDND = func(p model.Presence) bool { return dndStatus != "" && p.AggregatedStatus == dndStatus }
	isPresenting = func(p model.Presence) bool {
		return presentingStatus != "" && p.AggregatedStatus == presentingStatus
	}
	t.Cleanup(func() { isDND, isPresenting = origDND, origPresenting })
}
```

- [ ] **Step 2: Extend the `TestShouldPush` table with `dnd`/`presenting` columns**

Add `dnd` and `presenting` bool fields to the struct after `status`, replace the rows with the table below, and add `stubPresenceFlags(t, tt.dnd, tt.presenting)` as the first line of the subtest body.

```go
		// Zero notifSettings with both stubs inert must reproduce the pre-change
		// truth table exactly: no stored settings means no behaviour change.
		{"zero settings online", "online", false, false, notifSettings{}, false, true},
		{"zero settings offline", "offline", false, false, notifSettings{}, false, true},
		{"zero settings away", "away", false, false, notifSettings{}, false, true},
		{"zero settings busy", "busy", false, false, notifSettings{}, false, false},
		{"zero settings in-call", "in-call", false, false, notifSettings{}, false, false},
		{"zero settings missing status", "", false, false, notifSettings{}, false, true},
		{"zero settings unknown status", "unknown", false, false, notifSettings{}, false, true},

		// muteAll suppresses unless a priority sender pierces it.
		{"muted, no pierce", "online", false, false, notifSettings{muteAll: true}, false, false},
		{"muted, priority sender but pierce disabled", "online", false, false, notifSettings{muteAll: true}, true, false},
		{"muted, pierce enabled but sender not priority", "online", false, false, notifSettings{muteAll: true, allowPriority: true}, false, false},
		{"muted, pierce enabled and sender is priority", "online", false, false, notifSettings{muteAll: true, allowPriority: true}, true, true},
		{"unmuted, pierce enabled, non-priority sender", "online", false, false, notifSettings{allowPriority: true}, false, true},

		// Rule 2: DND and presenting suppress on their own, and the in-call opt-in
		// does not rescue them — showNotificationsInCall governs in-call only.
		{"dnd", "online", true, false, notifSettings{}, false, false},
		{"dnd, in-call opt-in does not rescue", "online", true, false, notifSettings{showInCall: true}, false, false},
		{"presenting", "online", false, true, notifSettings{}, false, false},
		{"presenting, in-call opt-in does not rescue", "online", false, true, notifSettings{showInCall: true}, false, false},
		{"dnd and presenting together", "online", true, true, notifSettings{}, false, false},

		// showNotificationsInCall governs the in-call bucket, for non-priority senders.
		{"in-call, opted in", "in-call", false, false, notifSettings{showInCall: true}, false, true},
		{"busy, opted in", "busy", false, false, notifSettings{showInCall: true}, false, true},
		{"in-call, not opted in", "in-call", false, false, notifSettings{}, false, false},

		// The pierce crosses every suppressor, DND and presenting included.
		{"dnd, priority pierce", "online", true, false, notifSettings{allowPriority: true}, true, true},
		{"presenting, priority pierce", "online", false, true, notifSettings{allowPriority: true}, true, true},
		{"in-call, priority pierce without in-call opt-in", "in-call", false, false, notifSettings{allowPriority: true}, true, true},
		{"muted+dnd, priority pierce", "in-call", true, false, notifSettings{muteAll: true, allowPriority: true}, true, true},

		// ...but only with its opt-in. A priority sender alone pierces nothing.
		{"dnd, priority sender but pierce disabled", "online", true, false, notifSettings{}, true, false},
		{"presenting, priority sender but pierce disabled", "online", false, true, notifSettings{}, true, false},
		{"in-call, priority sender but pierce disabled", "in-call", false, false, notifSettings{}, true, false},

		// Every suppressor clear.
		{"muted+pierced, online", "online", false, false, notifSettings{muteAll: true, allowPriority: true, showInCall: true}, true, true},
	}
```

- [ ] **Step 3: Add the stub-contract test**

`TestIsInCall` stays exactly as it is — it still asserts the `busy`+`in-call` bucket. Add alongside it:

```go
// TestDNDAndPresentingStubsAreInert pins the stub contract: until the presence
// side ships the real predicates, neither may infer a status from what we
// currently receive. A stub that starts returning true for "busy" or "in-call"
// would silently change delivery, so every status we know about is asserted.
func TestDNDAndPresentingStubsAreInert(t *testing.T) {
	for _, status := range []string{"busy", "in-call", "online", "offline", "away", "", "unknown"} {
		t.Run(status, func(t *testing.T) {
			p := model.Presence{AggregatedStatus: status}
			assert.False(t, isDND(p), "isDND must stay inert until the presence side owns it")
			assert.False(t, isPresenting(p), "isPresenting must stay inert until the presence side owns it")
		})
	}
}
```

- [ ] **Step 4: Add the two handler tests**

The table proves the pure function. These prove the handler consults both stubs and that the pierce is keyed on the **sender's** account — an easy-to-invert wiring detail no pure-function test can catch. Append to `handler_test.go`:

```go
// TestHandle_DNDAndPresentingSuppressPush proves the handler consults both stubs
// once they go live. Every recipient here has showNotificationsInCall set, so
// only a suppressor the opt-in does NOT govern can drop them — which is exactly
// rule 2. Uses invented statuses so the test asserts the wiring, not a mapping
// the presence side has yet to define.
func TestHandle_DNDAndPresentingSuppressPush(t *testing.T) {
	stubPresenceFlagsByStatus(t, "stub-dnd", "stub-presenting")

	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
			{ID: "dave", Account: "dave"},
		},
	}}
	presence := &stubPresence{out: map[string]model.Presence{
		"bob":   {AggregatedStatus: "stub-dnd"},
		"carol": {AggregatedStatus: "stub-presenting"},
		"dave":  {AggregatedStatus: "online"},
	}}
	settings := &stubSettings{out: map[string]notifSettings{
		"bob":   {showInCall: true},
		"carol": {showInCall: true},
		"dave":  {showInCall: true},
	}}
	emit := &recordingEmitter{}
	h := newTestHandlerWithSettings(members, presence, settings, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))
	assert.Equal(t, []string{"dave"}, emit.accounts(),
		"DND and presenting suppress regardless of showNotificationsInCall")
}

// TestHandle_PriorityContactPiercesDNDStub is the pierce counterpart: the same
// suppressed recipient survives when the sender is one of their priority contacts.
func TestHandle_PriorityContactPiercesDNDStub(t *testing.T) {
	stubPresenceFlagsByStatus(t, "stub-dnd", "")

	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
		},
	}}
	presence := &stubPresence{out: map[string]model.Presence{
		"bob":   {AggregatedStatus: "stub-dnd"},
		"carol": {AggregatedStatus: "stub-dnd"},
	}}
	settings := &stubSettings{out: map[string]notifSettings{
		"bob":   {allowPriority: true, priorityContacts: map[string]struct{}{"alice": {}}},
		"carol": {allowPriority: true, priorityContacts: map[string]struct{}{"dave": {}}},
	}}
	emit := &recordingEmitter{}
	h := newTestHandlerWithSettings(members, presence, settings, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))
	assert.Equal(t, []string{"bob"}, emit.accounts(),
		"bob lists the sender as a priority contact and is pierced out of DND; carol does not")
}
```

Also rename the existing `TestHandle_PriorityContactPiercesDND` (if present from an earlier draft) to `TestHandle_PriorityContactPiercesPresenceSuppression`, since with `isInCall` unchanged it exercises the `busy` bucket, not DND.

- [ ] **Step 5: Run the tests to verify they fail**

Run: `make test SERVICE=notification-worker`

Expected: FAIL with a compile error, `undefined: isDND`. That is a valid red phase — the identifier genuinely does not exist yet. Do not proceed until you have seen it.

- [ ] **Step 6: Add the stubs and rewrite the gate**

In `notification-worker/presence.go`, insert the stub vars immediately above `isInCall`, leave `isInCall` untouched, and rewrite `shouldPush`:

```go
// isDND and isPresenting are stubs: deriving these two statuses belongs to the
// presence side of the system, which has not shipped them yet. They are vars so
// tests can supply the eventual behaviour and prove the gate ordering now.
//
// Deliberately NOT inferred from any status we currently receive — mapping DND
// onto "busy" or presenting onto "in-call" would invent a contract we do not own.
// Until the real predicates land, both return false and rule 2 of the decision
// table is inert.
var (
	isDND        = func(model.Presence) bool { return false }
	isPresenting = func(model.Presence) bool { return false }
)
```

```go
// shouldPush applies the priority-contact pierce, then the suppressors in the
// issue's stated priority order. alwaysAllowPriorityNotifications is the single
// opt-in for "priority contacts reach me anyway", so the pierce crosses all of
// them. Unknown presence fails open.
func shouldPush(p model.Presence, ns notifSettings, isPrioritySender bool) bool {
	if ns.allowPriority && isPrioritySender {
		return true
	}
	if ns.muteAll {
		return false
	}
	if isDND(p) || isPresenting(p) {
		return false
	}
	if isInCall(p) && !ns.showInCall {
		return false
	}
	return true
}
```

Update `isInCall`'s doc comment to say the `busy`+`in-call` bucket holds *while `isDND` is inert*, so the next reader knows it is provisional.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `make test SERVICE=notification-worker`

Expected: PASS, everything — including the pre-existing `TestHandle_PresenceBusyDropsPush` and `TestIsInCall`, both of which must keep passing **unmodified**. If you find yourself editing either, `isInCall` was narrowed when it should not have been.

- [ ] **Step 8: Correct the stale comment in `handler.go`**

The comment block above the two `Snapshot` calls ends with a sentence that is no longer true — `showNotificationsInCall` is not the only thing modifying the presence decision. Replace only that trailing sentence:

```go
	// Both lookups run over the narrowed candidate set — only accounts that survived
	// the exclusion filters, never every member of a large room.
	// TestHandle_SettingsFetchedOnlyForSurvivingCandidates pins that narrowing.
	// shouldPush combines the two, keyed on the sender's account for the priority pierce.
	settings, _ := h.deps.Settings.Snapshot(ctx, accounts) // fail-open: error → empty map
	snapshot, _ := h.deps.Presence.Snapshot(ctx, accounts) // fail-open: error → empty map
```

- [ ] **Step 9: Verify coverage**

Run:

```bash
go test -race -coverprofile=coverage.out ./notification-worker/ && go tool cover -func=coverage.out | grep -E "shouldPush|isInCall"
```

Expected: `100.0%` on both. Delete `coverage.out` afterwards — it must not be committed.

- [ ] **Step 10: Lint**

Run: `make lint`

Expected: clean.

- [ ] **Step 11: Commit**

```bash
git add notification-worker/presence.go notification-worker/presence_test.go notification-worker/handler.go notification-worker/handler_test.go
git commit -m "feat(notification-worker): priority contacts pierce presence suppression

alwaysAllowPriorityNotifications becomes the single opt-in for piercing
mute and presence suppression alike. Adds inert isDND/isPresenting
predicates so the issue's do-not-disturb rule is wired and tested; the
presence side owns deriving those statuses, so nothing infers them from
busy/in-call and isInCall is unchanged.

Refs #221"
```

---

### Task 2: Correct the documentation the change invalidates

**Files:**
- Modify: `docs/client-api.md` — the `alwaysAllowPriorityNotifications` and `showNotificationsInCall` rows in the settings field table; the push-filter bullet in §4
- Modify: `docs/superpowers/specs/2026-08-10-notification-settings-enforcement-design.md` (head of file)

**Interfaces:** None — documentation only. No request/response schema, model struct or event changed, so `docs/client-api/request-reply.md` and `docs/client-api/events.md` are deliberately **not** touched.

Document only what the code does **today**. Rule 2 stays out of the client API surface: writing "do-not-disturb suppresses push" while `isDND` returns false would document behaviour clients cannot observe.

- [ ] **Step 1: Rewrite the `alwaysAllowPriorityNotifications` row**

Its final sentence currently reads "The pierce does not override `showNotificationsInCall`." Replace the whole row with:

```markdown
| `alwaysAllowPriorityNotifications` | boolean | Always allow priority-contact notifications. Enforced server-side: a message whose sender is in [`priorityContacts`](#settingsprioritycontactsadd) is pushed regardless of `muteAllNotifications` and regardless of a suppressing presence (`"busy"` / `"in-call"`) — in **any** room type, DM and channel alike, and for `.bot` senders as well as users. This setting is the only opt-in for that pierce; listing a priority contact without enabling it changes nothing. Per-room mute is not pierced. |
```

- [ ] **Step 2: Rewrite the `showNotificationsInCall` row**

Only the pierce sentence changes; the `busy` + `in-call` bucket stays accurate.

```markdown
| `showNotificationsInCall` | boolean | Show notifications in call. Enforced server-side: when unset or `false`, push is suppressed while the user's presence is `"busy"` or `"in-call"`. A priority-contact pierce bypasses this — see `alwaysAllowPriorityNotifications`. This enforcement takes effect once presence reporting is enabled server-side; until then no status is treated as in-call, so pushes are delivered regardless of this setting. |
```

- [ ] **Step 3: Add the pierce to the §4 push-filter bullets**

Keep the existing presence bullet's substance and add the pierce beneath it:

```markdown
- Presence-busy / in-call recipients are not pushed unless they set
  `showNotificationsInCall`; everyone else (online, offline, away, missing)
  receives one.
- A sender in the recipient's `priorityContacts` bypasses `muteAllNotifications`
  and presence suppression alike, but only when the recipient enabled
  `alwaysAllowPriorityNotifications`. Per-room mute is never bypassed.
```

- [ ] **Step 4: Mark Spec 2 partly superseded**

Insert immediately below the `# Notification Settings Enforcement (Spec 2 of 2)` heading:

```markdown
> **Partly superseded** by `2026-08-10-priority-contact-presence-gating-design.md`
> (Spec 3, issue #221), which reverses one decision below: the priority-contact
> pierce now *does* cross the in-call gate. The `busy`+`in-call` bucket described
> here is still what ships; Spec 3 adds inert `isDND`/`isPresenting` predicates
> above it, to be filled in by the presence side of the system. Everything else
> here — placement, fail-open, the no-cache reasoning, config — still describes
> the shipped code.
```

- [ ] **Step 5: Verify no derived view drifted**

Run:

```bash
grep -n "showNotificationsInCall\|alwaysAllowPriorityNotifications" docs/client-api/request-reply.md docs/client-api/events.md
```

Expected: matches that merely name the field in a schema table without describing enforcement. If a view *describes* enforcement semantics, update it to match Steps 1-2.

- [ ] **Step 6: Commit**

```bash
git add docs/client-api.md docs/superpowers/specs/2026-08-10-notification-settings-enforcement-design.md
git commit -m "docs(client-api): priority-pierce bypasses presence suppression

Refs #221"
```

---

### Task 3: Whole-repo verification

**Files:** none modified — this task only runs gates.

- [ ] **Step 1: Full unit suite with race detector**

Run: `make test`

Expected: PASS. `shouldPush` is called only from `notification-worker`, so a failure elsewhere means something unintended was touched.

- [ ] **Step 2: Lint**

Run: `make lint`

Expected: clean.

- [ ] **Step 3: SAST**

Run: `make sast`

Expected: no medium-or-above findings. Note that in a sandboxed session `govulncheck` (needs `vuln.go.dev`) and semgrep's registry rulesets (need `semgrep.dev`) may be blocked by egress policy — report that rather than routing around it, and run the repo-local rules alone:

```bash
semgrep --error --severity=WARNING --severity=ERROR --metrics=off \
  --exclude=tools --exclude=chat-frontend --exclude=testdata --exclude=docs \
  --config=.semgrep/ ./notification-worker/
```

- [ ] **Step 4: Confirm the diff matches the spec's scope**

Run: `git diff --stat origin/main...HEAD`

Expected files and no others: `notification-worker/presence.go`, `notification-worker/presence_test.go`, `notification-worker/handler.go`, `notification-worker/handler_test.go`, `docs/client-api.md`, the two spec files, and this plan. Confirm **no** change to `notification-worker/main.go`, `usersettings.go`, either `deploy/*/docker-compose.yml`, or anything under `pkg/`. Confirm no `coverage.out` was committed.

- [ ] **Step 5: Push**

```bash
git push -u origin claude/issue-221-design-plan-qxdqv5
```

Retry up to 4 times with exponential backoff (2s, 4s, 8s, 16s) on network failure only.

---

## Follow-up (not this plan)

When the presence side ships do-not-disturb and presenting, that change:

1. Replaces the two stub vars with real `func isDND(p model.Presence) bool` / `func isPresenting(p model.Presence) bool`.
2. Drops `"busy"` from `isInCall`, leaving it `in-call` only.
3. Deletes `TestDNDAndPresentingStubsAreInert` and converts the test helpers if the vars become plain funcs.
4. Adds the do-not-disturb sentence to the `showNotificationsInCall` row in `docs/client-api.md`.
5. Sizes `{"settings.showNotificationsInCall": true, "active": {"$ne": false}}` in production first — those users stop receiving pushes while in do-not-disturb, the one delivery reduction in this whole series.

**Sequence it consumer-first.** `shouldPush` fails open on unrecognized presence and `isInCall` matches literal status strings. If a producer *replaces* `busy` with a new do-not-disturb representation while any `notification-worker` is still on the old binary, that worker fails open and pushes to users who are suppressed today — a live regression. Roll recognition out to every worker before any producer emits the new representation, and keep emitting `busy` alongside it until the last old worker and every rollback target is gone. Note that old workers still cannot suppress do-not-disturb for users with `showNotificationsInCall: true` — the opt-in outranks the whole bucket under that binary, and no encoding changes it. Those users keep today's behaviour until rollout completes; that is a delayed benefit, not a regression, and dropping `busy` to chase it would cause one.
