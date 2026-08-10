# Priority-Contact Presence Gating Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the priority-contact pierce cross the presence gate, and stop `showNotificationsInCall` from governing manual do-not-disturb.

**Architecture:** All behaviour lives in one pure function, `shouldPush` in `notification-worker/presence.go`. Its single predicate `isInCall` — which today buckets `busy` and `in-call` together — splits into `isDND` and `isInCall`, and the pierce moves to the top of the function as a total early return. Nothing else in the pipeline changes: the candidate loop, the settings/presence snapshots, survivor sorting, batching and dedup are untouched.

**Tech Stack:** Go 1.25, `stretchr/testify` for assertions, table-driven tests, `make` targets.

**Spec:** `docs/superpowers/specs/2026-08-10-priority-contact-presence-gating-design.md`

## Global Constraints

- TDD is mandatory (CLAUDE.md §4): write the tests, run them, **confirm they fail**, then implement. Task 1 is ordered so every test exists before any implementation.
- Use `make` targets, not raw `go` commands — except for coverage, where CLAUDE.md itself prescribes `go test -coverprofile` / `go tool cover -func`.
- Unit tests for this service: `make test SERVICE=notification-worker` (expands to `go test -race ./notification-worker/...`). The target does **not** forward `-run`; to run a single test use `go test -race ./notification-worker/ -run '<Name>' -v` directly, and finish with the `make` target.
- Test files stay in `package main` so they can reach unexported identifiers.
- Presence statuses come from `pkg/model` constants — `model.StatusBusy` (`"busy"`), `model.StatusInCall` (`"in-call"`). Compare via `string(...)`; `model.Presence.AggregatedStatus` is a plain `string`.
- Comments follow the repo's WHY-not-WHAT convention: max 2 lines, rationale only, never restating the code.
- Minimum 80% coverage; `shouldPush`, `isDND` and `isInCall` must each reach 100%.
- Lint and tests are enforced by a pre-commit hook. A failing hook means fix, not retry.
- Do NOT add env vars, model fields, RPCs or events. `USER_SETTINGS_ENABLED=false` is already the kill switch for everything here.
- Do NOT modify `handler.go`'s candidate loop. A per-room-muted member must keep being dropped before the gate — no pierce resurrects them.

---

### Task 1: Split the presence predicates and make the pierce total

Every test lands before the implementation, so the red phase is real. The two handler tests are here rather than in a later task for exactly that reason: written after the implementation they would pass on arrival and prove nothing.

**Files:**
- Modify: `notification-worker/presence.go:119-143` (`isInCall`, `shouldPush`)
- Modify: `notification-worker/handler.go:200-207` (comment block above the two `Snapshot` calls)
- Test: `notification-worker/presence_test.go:27-89` (`TestShouldPush`, `TestIsInCall`)
- Test: `notification-worker/handler_test.go` (append two tests at end of file)

**Interfaces:**
- Consumes: `notifSettings` from `notification-worker/usersettings.go` — fields `muteAll`, `allowPriority`, `showInCall bool`, `priorityContacts map[string]struct{}`. `model.Presence{AggregatedStatus string}` from `pkg/model/presence.go`. Existing test fixtures in `handler_test.go`: `newTestHandlerWithSettings(members, presence, settings, hook, emit)`, `stubMembers{out map[string][]roomsubcache.Member}`, `stubPresence{out map[string]model.Presence}`, `stubSettings{out map[string]notifSettings, err error}`, `recordingEmitter` with `.accounts() []string`, `msgEvent(*model.Message) []byte`, `noopVetoer{}`.
- Produces: `func isDND(p model.Presence) bool`, `func isInCall(p model.Presence) bool`, `func shouldPush(p model.Presence, ns notifSettings, isPrioritySender bool) bool` — **signature unchanged**, so `handler.go:213`'s call site keeps compiling untouched.

- [ ] **Step 1: Replace the `TestShouldPush` table**

Replace the whole `tests := []struct{...}{...}` literal in `TestShouldPush` (`presence_test.go:28-62`) with the table below. Keep the surrounding `for`/`t.Run` loop exactly as it is.

```go
	tests := []struct {
		name             string
		status           string
		ns               notifSettings
		isPrioritySender bool
		want             bool
	}{
		// Zero notifSettings must reproduce pre-Spec-3 behaviour on every status:
		// no stored settings means no behaviour change from this deploy.
		{"zero settings online", "online", notifSettings{}, false, true},
		{"zero settings offline", "offline", notifSettings{}, false, true},
		{"zero settings away", "away", notifSettings{}, false, true},
		{"zero settings busy", "busy", notifSettings{}, false, false},
		{"zero settings in-call", "in-call", notifSettings{}, false, false},
		{"zero settings missing status", "", notifSettings{}, false, true},
		{"zero settings unknown status", "unknown", notifSettings{}, false, true},

		// muteAll suppresses unless a priority sender pierces it.
		{"muted, no pierce", "online", notifSettings{muteAll: true}, false, false},
		{"muted, priority sender but pierce disabled", "online", notifSettings{muteAll: true}, true, false},
		{"muted, pierce enabled but sender not priority", "online", notifSettings{muteAll: true, allowPriority: true}, false, false},
		{"muted, pierce enabled and sender is priority", "online", notifSettings{muteAll: true, allowPriority: true}, true, true},
		{"unmuted, pierce enabled, non-priority sender", "online", notifSettings{allowPriority: true}, false, true},

		// DND is no longer governed by showNotificationsInCall. This row is the one
		// population whose pushes this change removes.
		{"busy, opted in to in-call notifications, still suppressed", "busy", notifSettings{showInCall: true}, false, false},
		{"busy, no opt-in", "busy", notifSettings{}, false, false},

		// showNotificationsInCall still governs in-call, for every non-priority sender.
		{"in-call, opted in", "in-call", notifSettings{showInCall: true}, false, true},
		{"in-call, not opted in", "in-call", notifSettings{}, false, false},

		// The pierce now crosses the presence gate too — the Spec 3 reversal.
		{"busy, priority pierce", "busy", notifSettings{allowPriority: true}, true, true},
		{"in-call, priority pierce without in-call opt-in", "in-call", notifSettings{allowPriority: true}, true, true},
		{"muted+pierced, in-call without opt-in", "in-call", notifSettings{muteAll: true, allowPriority: true}, true, true},
		{"muted+pierced, in-call with opt-in", "in-call", notifSettings{muteAll: true, allowPriority: true, showInCall: true}, true, true},

		// ...but only with its opt-in. A priority sender alone pierces nothing.
		{"busy, priority sender but pierce disabled", "busy", notifSettings{}, true, false},
		{"in-call, priority sender but pierce disabled", "in-call", notifSettings{}, true, false},

		// Every suppressor clear.
		{"muted+pierced, online", "online", notifSettings{muteAll: true, allowPriority: true, showInCall: true}, true, true},
	}
```

- [ ] **Step 2: Split `TestIsInCall` into two tests**

Replace `TestIsInCall` (`presence_test.go:71-89`) entirely with the pair below. Each asserts the *other* status is false — that mutual exclusion is what fails if anyone re-merges the bucket.

```go
func TestIsDND(t *testing.T) {
	tests := []struct {
		status string
		want   bool
	}{
		{"busy", true},
		{"in-call", false},
		{"online", false},
		{"offline", false},
		{"away", false},
		{"", false},
		{"unknown", false},
	}
	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			assert.Equal(t, tt.want, isDND(model.Presence{AggregatedStatus: tt.status}))
		})
	}
}

func TestIsInCall(t *testing.T) {
	tests := []struct {
		status string
		want   bool
	}{
		{"in-call", true},
		{"busy", false},
		{"online", false},
		{"offline", false},
		{"away", false},
		{"", false},
		{"unknown", false},
	}
	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			assert.Equal(t, tt.want, isInCall(model.Presence{AggregatedStatus: tt.status}))
		})
	}
}
```

- [ ] **Step 3: Add the two handler tests**

The table above proves the pure function. These prove the handler actually combines the settings map and the presence map per-recipient — `ns.isPriority(msg.UserAccount)` is keyed on the **sender's** account, an easy-to-invert wiring detail no pure-function test can catch.

Append to the end of `notification-worker/handler_test.go`:

```go
// TestHandle_PriorityContactPiercesDND pins the Spec 3 reversal end-to-end: the
// pierce crosses the presence gate, and it is keyed on the sender's account —
// bob lists the sender, carol lists someone else, and both are equally in DND.
func TestHandle_PriorityContactPiercesDND(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
		},
	}}
	presence := &stubPresence{out: map[string]model.Presence{
		"bob":   {AggregatedStatus: "busy"},
		"carol": {AggregatedStatus: "busy"},
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

// TestHandle_DNDIgnoresShowNotificationsInCall pins the other half of the split:
// the in-call opt-in must not rescue a recipient who is in do-not-disturb.
func TestHandle_DNDIgnoresShowNotificationsInCall(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice"},
			{ID: "bob", Account: "bob"},
			{ID: "carol", Account: "carol"},
		},
	}}
	presence := &stubPresence{out: map[string]model.Presence{
		"bob":   {AggregatedStatus: "busy"},
		"carol": {AggregatedStatus: "in-call"},
	}}
	settings := &stubSettings{out: map[string]notifSettings{
		"bob":   {showInCall: true},
		"carol": {showInCall: true},
	}}
	emit := &recordingEmitter{}
	h := newTestHandlerWithSettings(members, presence, settings, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice", CreatedAt: time.Now(),
	})))
	assert.Equal(t, []string{"carol"}, emit.accounts(),
		"showNotificationsInCall covers in-call only; bob stays suppressed by DND")
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `make test SERVICE=notification-worker`

Expected: FAIL. The first failure is a compile error, `undefined: isDND`, because the identifier genuinely does not exist yet — that is a valid red phase, not a setup problem. Do not proceed until you have seen it.

- [ ] **Step 5: Rewrite the predicates and the gate**

In `notification-worker/presence.go`, replace `isInCall` and `shouldPush` — everything from the `// isInCall reports whether presence alone suppresses push.` comment down to the closing brace of `shouldPush` (lines 119-143) — with:

```go
// isDND reports manual do-not-disturb. Split from isInCall because
// showNotificationsInCall governs only the latter — DND means DND.
func isDND(p model.Presence) bool {
	return p.AggregatedStatus == string(model.StatusBusy)
}

// isInCall reports an active call. Teams "Presenting" arrives here too:
// user-presence-service folds it into in-call rather than modelling it separately.
func isInCall(p model.Presence) bool {
	return p.AggregatedStatus == string(model.StatusInCall)
}

// shouldPush applies the priority-contact pierce, then three independent
// suppressors. alwaysAllowPriorityNotifications is the single opt-in for
// "priority contacts reach me anyway", so the pierce crosses all three.
func shouldPush(p model.Presence, ns notifSettings, isPrioritySender bool) bool {
	if ns.allowPriority && isPrioritySender {
		return true
	}
	if ns.muteAll {
		return false
	}
	if isDND(p) {
		return false
	}
	if isInCall(p) && !ns.showInCall {
		return false
	}
	return true
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `make test SERVICE=notification-worker`

Expected: PASS, everything — including the pre-existing `TestHandle_PresenceBusyDropsPush`, where bob is `busy` with zero settings and must still be dropped. That test must keep passing **unmodified**; if you find yourself editing it, the implementation is wrong.

- [ ] **Step 7: Correct the stale comment in `handler.go`**

The comment block above the two `Snapshot` calls (`handler.go:200-207`) ends with a sentence that is no longer true — `showNotificationsInCall` is not the only thing modifying the presence decision. Replace only that trailing sentence, keeping the narrowing lines intact:

```go
	// Both lookups run over the narrowed candidate set — only accounts that survived
	// the exclusion filters, never every member of a large room.
	// TestHandle_SettingsFetchedOnlyForSurvivingCandidates pins that narrowing.
	// shouldPush combines the two, keyed on the sender's account for the priority pierce.
	settings, _ := h.deps.Settings.Snapshot(ctx, accounts) // fail-open: error → empty map
	snapshot, _ := h.deps.Presence.Snapshot(ctx, accounts) // fail-open: error → empty map
```

- [ ] **Step 8: Verify coverage of the changed functions**

Run:

```bash
go test -race -coverprofile=coverage.out ./notification-worker/ && go tool cover -func=coverage.out | grep -E "shouldPush|isDND|isInCall"
```

Expected: `100.0%` on all three lines. Every branch has a table row, so anything less means a row is missing. Delete `coverage.out` afterwards — it must not be committed.

- [ ] **Step 9: Run the linter**

Run: `make lint`

Expected: clean.

- [ ] **Step 10: Commit**

```bash
git add notification-worker/presence.go notification-worker/presence_test.go notification-worker/handler.go notification-worker/handler_test.go
git commit -m "feat(notification-worker): priority contacts pierce DND and in-call

DND (presence \"busy\") is no longer governed by showNotificationsInCall,
which now covers in-call only. alwaysAllowPriorityNotifications becomes the
single opt-in for piercing mute, DND and in-call alike; per-room mute is
still never pierced.

Refs #221"
```

---

### Task 2: Correct the documentation the change invalidates

Three pieces of prose become factually wrong the moment Task 1 merges. CLAUDE.md requires `docs/client-api.md` to track client-facing behaviour in the same PR.

**Files:**
- Modify: `docs/client-api.md` — the `alwaysAllowPriorityNotifications` and `showNotificationsInCall` rows in the settings field table (~lines 4760-4763); the push-filter bullet in §4 (~lines 6449-6450)
- Modify: `docs/superpowers/specs/2026-08-10-notification-settings-enforcement-design.md` (head of file)

**Interfaces:** None — documentation only. No request/response schema, model struct or event changed, so `docs/client-api/request-reply.md` and `docs/client-api/events.md` are deliberately **not** touched.

- [ ] **Step 1: Rewrite the `alwaysAllowPriorityNotifications` row**

Find the row beginning `| \`alwaysAllowPriorityNotifications\` | boolean |`. Its final sentence currently reads "The pierce does not override `showNotificationsInCall`." Replace the entire row with:

```markdown
| `alwaysAllowPriorityNotifications` | boolean | Always allow priority-contact notifications. Enforced server-side: a message whose sender is in [`priorityContacts`](#settingsprioritycontactsadd) is pushed regardless of `muteAllNotifications`, of a `"busy"` (do-not-disturb) presence, and of an `"in-call"` presence — in **any** room type, DM and channel alike, and for `.bot` senders as well as users. This setting is the only opt-in for that pierce; listing a priority contact without enabling it changes nothing. Per-room mute is not pierced. |
```

- [ ] **Step 2: Rewrite the `showNotificationsInCall` row**

Find the row beginning `| \`showNotificationsInCall\` | boolean |`. It currently claims the setting covers `"busy"` **or** `"in-call"`, and that the priority pierce does not bypass it — both now wrong. Replace the entire row with:

```markdown
| `showNotificationsInCall` | boolean | Show notifications in call. Enforced server-side: when unset or `false`, push is suppressed while the user's presence is `"in-call"`. It does **not** govern `"busy"` (do-not-disturb), which suppresses push either way — see `alwaysAllowPriorityNotifications` for the only exemption. A priority-contact pierce does bypass this setting. This enforcement takes effect once presence reporting is enabled server-side; until then no status is treated as in-call, so pushes are delivered regardless of this setting. |
```

- [ ] **Step 3: Rewrite the §4 push-filter bullet**

Find the bullet reading "Presence-busy / in-call recipients are not pushed; everyone else (online, offline, away, missing) receives one." Replace those two lines with:

```markdown
- Recipients whose presence is `busy` (do-not-disturb) are not pushed; `in-call`
  recipients are not pushed unless they set `showNotificationsInCall`. Everyone
  else (online, offline, away, missing) receives one.
- A sender in the recipient's `priorityContacts` bypasses `muteAllNotifications`,
  `busy` and `in-call`, but only when the recipient enabled
  `alwaysAllowPriorityNotifications`. Per-room mute is never bypassed.
```

- [ ] **Step 4: Mark Spec 2 partly superseded**

Insert immediately below the `# Notification Settings Enforcement (Spec 2 of 2)` heading in `docs/superpowers/specs/2026-08-10-notification-settings-enforcement-design.md`:

```markdown
> **Partly superseded** by `2026-08-10-priority-contact-presence-gating-design.md`
> (Spec 3, issue #221), which reverses two decisions below: the priority-contact
> pierce now *does* cross the in-call gate, and `showNotificationsInCall` no longer
> governs `busy`/do-not-disturb. Everything else here — placement, fail-open, the
> no-cache reasoning, config — still describes the shipped code.
```

- [ ] **Step 5: Verify no derived view drifted**

Run:

```bash
grep -n "showNotificationsInCall\|alwaysAllowPriorityNotifications" docs/client-api/request-reply.md docs/client-api/events.md
```

Expected: no matches, or matches that merely name the field in a schema table without describing enforcement. If a derived view *describes* enforcement semantics, update it to match Steps 1-2 — CLAUDE.md forbids letting the views drift from the canonical doc.

- [ ] **Step 6: Commit**

```bash
git add docs/client-api.md docs/superpowers/specs/2026-08-10-notification-settings-enforcement-design.md
git commit -m "docs(client-api): DND and priority-pierce enforcement semantics

Refs #221"
```

---

### Task 3: Whole-repo verification

**Files:** none modified — this task only runs gates.

- [ ] **Step 1: Full unit suite with race detector**

Run: `make test`

Expected: PASS. `shouldPush` is called only from `notification-worker`, so nothing outside it should move; a failure elsewhere means something unintended was touched.

- [ ] **Step 2: Lint**

Run: `make lint`

Expected: clean.

- [ ] **Step 3: SAST**

Run: `make sast`

Expected: no medium-or-above findings. This change adds no I/O, parsing or crypto, so a new finding means something unrelated leaked into the branch.

- [ ] **Step 4: Confirm the diff matches the spec's scope**

Run: `git diff --stat main...HEAD`

Expected files and no others: `notification-worker/presence.go`, `notification-worker/presence_test.go`, `notification-worker/handler.go`, `notification-worker/handler_test.go`, `docs/client-api.md`, the two spec files, and this plan. Specifically confirm **no** change to `notification-worker/main.go`, `notification-worker/usersettings.go`, either `notification-worker/deploy/*/docker-compose.yml`, or anything under `pkg/` — the spec adds no config and no wire surface. Also confirm no `coverage.out` was committed.

- [ ] **Step 5: Push**

```bash
git push -u origin claude/issue-221-design-plan-qxdqv5
```

Retry up to 4 times with exponential backoff (2s, 4s, 8s, 16s) on network failure only.
