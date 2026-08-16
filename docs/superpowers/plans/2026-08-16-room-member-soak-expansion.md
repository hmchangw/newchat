# Room/Member Soak Lane Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the existing continuous `phase: soak` workload with four new room/member traffic lanes (`member_mutation`, `room_mutation`, `room_read`, `room_create`) whose asynchronous mutations are tracked end-to-end by the durable operation evidence ledger introduced in #271.

**Architecture:** The evidence ledger is generalized from a single hard-coded `message_send` lane to a per-lane observer contract, so each lane declares its own required observers. A new `room_state` observer reconciles room/member final state through two sources — the room-service RPC (production path) and a MongoDB primary read (authoritative arbiter) — and only claims data loss when room-service authoritatively accepted the request. A per-room candidate ring with leases and a bounded quarantine keeps member add/remove cycling forever without exhausting accounts or issuing conflicting operations against the same room/account.

**Tech Stack:** Go 1.25, `nats.go` request/reply, `mongo-driver/v2`, Prometheus client, `stretchr/testify`, `go.uber.org/mock`, Helm.

## Global Constraints

- Every new lane rides the existing `seed → soak → stopped → teardown` lifecycle. **No new Helm phase, no new Deployment, no new run mode.**
- No high-cardinality Prometheus labels. Room IDs, accounts, message IDs, operation IDs, and run IDs go to the WAL/log only, never to a metric label.
- All new metric label values come from bounded registries declared in code.
- `not_sent` is reserved for failures proven to have never left the loadgen process (marshal failure, closed connection before write). A NATS timeout or any ambiguous transport error is **never** `not_sent` and is **never** blindly retried.
- `missing_after_deadline` may only be claimed when the admission observer recorded `good`. This rule is already implemented generically in `failureOperationResult` (`tools/loadgen/failure_ledger.go:1310`) and must not be bypassed.
- Read-only lanes (`room_read`) emit latency/error/result metrics only. They do not create ledger operations.
- All commands go through `make` targets. Never run raw `go` commands.
- New `soak_*.go` files must reach **90%** statement coverage (`make coverage-loadgen-soak` gate, which excludes only `soak_main.go` and `soak_store.go`). New `failure_*.go` files must reach **80%**.
- `make coverage-loadgen-soak` runs `go test -run Soak`, so every new unit test in a `soak_*_test.go` file **must have `Soak` in its name**. `make coverage-loadgen-failure` runs the pattern `Failure|ObservationRuntime|Observer|Recipient|ConsumerSampler|SoakCatalog|SoakSender|SoakRuntimeSelector|SoakPacing|LoadgenNATSHealth`, so ledger tests must contain `Failure` or `Observer`.
- Lint (`make lint`) and tests are enforced by a pre-commit hook. `make sast` must pass before push.
- Errors are wrapped with context: `fmt.Errorf("short description: %w", err)`. Never return a bare `err`.
- Logging is `log/slog` with key-value fields. Never log room content, tokens, or full message bodies.

## Decisions already made (do not relitigate)

1. **One PR**, ordered commits, branch `claude/room-member-soak-expansion-6l6qq3` from latest `main`.
2. **Observer contract v2 is per-lane.** Bumping the contract version makes an existing WAL for the same run ID incompatible; that stays a hard startup failure (no silent memory-ledger fallback).
3. **`SOAK_LEDGER_EPOCH` splits identity:** `runId` identifies the seeded topology, `epoch` identifies the evidence journal. WAL path becomes `{dir}/{runId}.{epoch}.wal`, so upgrading the image does **not** require re-seeding 10k rooms — the operator bumps `ledger.epoch` instead. Older journals for the same run ID are retained, never replayed, and counted in a gauge.
4. **Candidate quarantine never invalidates the ledger.** Ledger invalidation is a sticky, one-way, process-lifetime latch (`failure_ledger.go:1198`); using it for a recoverable traffic degradation would poison a multi-day campaign. Degradation is expressed with reversible metrics instead.
5. **The `room_state` observer has two sources inside one observer** (room-service RPC + Mongo primary). It is a single required observer because both sources observe the *same* effect; per-source visibility comes from a separate bounded counter.
6. **Rooms created by the `room_create` lane never join the message/read topology.** They are read-only targets for `room_read`. Reasons: recipient-observer subscriptions are built once at startup from the seeded topology, the send-side room picker is a fixed distribution built at startup, new rooms need asynchronous DEK provisioning before sends, and the pinned catalog is warmed at startup.

---

## File Structure

**New files**

| File | Responsibility |
|---|---|
| `tools/loadgen/soak_roomstate.go` | Pure in-memory state: per-room candidate ring, `(room, account)` leases, bounded quarantine, room name/mute cursor. No I/O. |
| `tools/loadgen/soak_roomstate_test.go` | Unit tests for the above. |
| `tools/loadgen/soak_roomops.go` | `soakRoomMutator`: member add/remove, rename, mute toggle, create room — each an `soakRPCClient` call returning a typed outcome. |
| `tools/loadgen/soak_roomops_test.go` | Unit tests with a fake transport. |
| `tools/loadgen/soak_roomread.go` | `soakRoomReader`: member list, rooms-info batch, subscription list, and the state readback used by reconciliation. |
| `tools/loadgen/soak_roomread_test.go` | Unit tests. |
| `tools/loadgen/soak_roomverify.go` | `soakRoomStateVerifier`: the `room_state` observer body. Resolves an operation's expected final state through room-service then Mongo primary. |
| `tools/loadgen/soak_roomverify_test.go` | Unit tests for every verdict combination. |
| `tools/loadgen/soak_roommember.go` | Lane glue: intent journaling, admission observation, reconciliation driver, room-create budget. |
| `tools/loadgen/soak_roommember_test.go` | Unit tests. |

**Modified files**

| File | Change |
|---|---|
| `tools/loadgen/failure_ledger.go` | Observer contract v2 (per-lane), lane/scenario/operation-type registries, WAL path epoch. |
| `tools/loadgen/failure_observer.go` | Register `room_state` observer + new effects. |
| `tools/loadgen/soak_failure.go` | Build the per-lane contract; WAL path helper; new lane constants. |
| `tools/loadgen/soak_config.go` | New env vars + validation. |
| `tools/loadgen/soak_workload.go` | Four new lanes in `soakWorkloadActions`/`lanes()`. |
| `tools/loadgen/soak_main.go` | Wire the new components. |
| `tools/loadgen/soak_rpc.go` | New bounded `soakRPCAction` values. |
| `tools/loadgen/soak_wire.go` | Request/response carriers for the room RPCs. |
| `tools/loadgen/soak_store.go` | Mongo primary readbacks + ownership append. |
| `tools/loadgen/metrics.go` | New bounded metric families. |
| `tools/loadgen/deploy/k8s/values.yaml`, `values.schema.json`, `templates/configmap.yaml`, `templates/_helpers.tpl` | New lane rates, budget, epoch. |
| `tools/loadgen/deploy/docker-compose.yml` | Same knobs for local runs. |
| `docs/load-testing/loadgen-failure-observation.md` | Document the new lanes, epoch, quarantine, and gates. |
| `tools/loadgen/README.md` | Operator-facing summary. |

---

## Shared vocabulary (used by every task)

```go
// Lanes (soak_failure.go)
soakFailureLaneMessageSend    = "message_send"     // existing
soakFailureLaneMemberMutation = "member_mutation"
soakFailureLaneRoomMutation   = "room_mutation"
soakFailureLaneRoomCreate     = "room_create"

// Operation types (failure_ledger.go)
failureOperationMessageCreate failureOperationType = "message_create" // existing
failureOperationMemberAdd     failureOperationType = "member_add"
failureOperationMemberRemove  failureOperationType = "member_remove"
failureOperationRoomRename    failureOperationType = "room_rename"
failureOperationMuteToggle    failureOperationType = "mute_toggle"
failureOperationRoomCreate    failureOperationType = "room_create"

// Effects (failure_ledger.go)
failureEffectMemberState      failureEffect = "member_state"
failureEffectRoomName         failureEffect = "room_name"
failureEffectSubscriptionMute failureEffect = "subscription_mute"
failureEffectRoomCreated      failureEffect = "room_created"

// Observer (failure_observer.go)
failureObserverRoomState failureObserver = "room_state"

// Reasons (failure_ledger.go)
failureReasonMemberStateMismatch failureReason = "member_state_mismatch"
failureReasonRoomNameMismatch    failureReason = "room_name_mismatch"
failureReasonMuteStateMismatch   failureReason = "mute_state_mismatch"
failureReasonRoomStateMissing    failureReason = "room_state_missing"

// RPC actions (soak_rpc.go)
soakRPCMemberAdd        soakRPCAction = "member_add"
soakRPCMemberRemove     soakRPCAction = "member_remove"
soakRPCRoomRename       soakRPCAction = "room_rename"
soakRPCMuteToggle       soakRPCAction = "mute_toggle"
soakRPCRoomCreate       soakRPCAction = "room_create"
soakRPCMemberList       soakRPCAction = "member_list"
soakRPCRoomsInfo        soakRPCAction = "rooms_info"
soakRPCSubscriptionList soakRPCAction = "subscription_list"
soakRPCRoomStateRead    soakRPCAction = "room_state_read"
```

Per-lane observer sets in the contract:

| Lane | Required observers |
|---|---|
| `message_send` | `admission`, `cassandra_history` (+ `recipient_broadcast` when enabled) |
| `member_mutation` | `admission`, `room_state` |
| `room_mutation` | `admission`, `room_state` |
| `room_create` | `admission`, `room_state` |

---

### Task 1: Generalize the ledger — registries, per-lane observer contract, WAL epoch

**Files:**
- Modify: `tools/loadgen/failure_ledger.go` (contract struct :28-45, registries :162-168, `validateFailureOperation` :1208-1251, `validateFailureObserverContract` :1581, `equalFailureObserverContract` :1598, `failureOperationMatchesObserverContract` :1614)
- Modify: `tools/loadgen/failure_observer.go` (registry :29-33)
- Modify: `tools/loadgen/soak_failure.go` (`openSoakFailureLedger` :73-113)
- Test: `tools/loadgen/failure_ledger_test.go`, `tools/loadgen/failure_ledger_review_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `func newFailureObserverContract(recipientEnabled bool) failureObserverContract` — unchanged signature, now populates `Lanes`.
  - `failureObserverContract{SchemaVersion int; Scenario string; Observers []failureObserver; Lanes map[string][]failureObserver; RecipientObserverEnabled bool}`
  - `func failureWALPath(dir, runID, epoch string) string`
  - `func memberMutationExpectedEffects(add bool) []failureExpectedEffect`
  - `func roomMutationExpectedEffects(operationType failureOperationType) []failureExpectedEffect`
  - `func roomCreateExpectedEffects() []failureExpectedEffect`
  - New constants listed in "Shared vocabulary".

- [ ] **Step 1: Write the failing contract test**

Add to `tools/loadgen/failure_ledger_test.go`:

```go
func TestFailureObserverContract_DeclaresPerLaneObservers(t *testing.T) {
	contract := newFailureObserverContract(false)

	assert.Equal(t, 2, contract.SchemaVersion)
	assert.Equal(t, []failureObserver{failureObserverAdmission, failureObserverHistory},
		contract.Lanes[soakFailureLaneMessageSend])
	assert.Equal(t, []failureObserver{failureObserverAdmission, failureObserverRoomState},
		contract.Lanes[soakFailureLaneMemberMutation])
	assert.Equal(t, []failureObserver{failureObserverAdmission, failureObserverRoomState},
		contract.Lanes[soakFailureLaneRoomMutation])
	assert.Equal(t, []failureObserver{failureObserverAdmission, failureObserverRoomState},
		contract.Lanes[soakFailureLaneRoomCreate])
	require.NoError(t, validateFailureObserverContract(contract))
}

func TestFailureObserverContract_RecipientOnlyAffectsMessageLane(t *testing.T) {
	contract := newFailureObserverContract(true)

	assert.Contains(t, contract.Lanes[soakFailureLaneMessageSend], failureObserverRecipient)
	assert.NotContains(t, contract.Lanes[soakFailureLaneMemberMutation], failureObserverRecipient)
	require.NoError(t, validateFailureObserverContract(contract))
}

func TestFailureOperationMatchesObserverContract_UsesOperationLane(t *testing.T) {
	contract := newFailureObserverContract(false)
	operation := failureOperation{
		Scenario: soakFailureScenario,
		Lane:     soakFailureLaneMemberMutation,
		Expected: []failureObserver{failureObserverAdmission, failureObserverRoomState},
	}

	assert.True(t, failureOperationMatchesObserverContract(&operation, contract))

	operation.Lane = soakFailureLaneMessageSend
	assert.False(t, failureOperationMatchesObserverContract(&operation, contract))
}

func TestFailureWALPath_SeparatesRunAndEpoch(t *testing.T) {
	assert.Equal(t, filepath.Join("/ledger", "run-1.v2.wal"), failureWALPath("/ledger", "run-1", "v2"))
	assert.Equal(t, filepath.Join("/ledger", "run-1.v1.wal"), failureWALPath("/ledger", "run-1", ""))
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL — `undefined: failureObserverRoomState`, `undefined: soakFailureLaneMemberMutation`, `undefined: failureWALPath`, and `contract.Lanes` undefined.

- [ ] **Step 3: Add the new bounded vocabulary**

In `tools/loadgen/failure_ledger.go`, replace the single operation-type constant (`:49`) and extend the effect/reason/registry blocks:

```go
type failureOperationType string

const (
	failureOperationMessageCreate failureOperationType = "message_create"
	failureOperationMemberAdd     failureOperationType = "member_add"
	failureOperationMemberRemove  failureOperationType = "member_remove"
	failureOperationRoomRename    failureOperationType = "room_rename"
	failureOperationMuteToggle    failureOperationType = "mute_toggle"
	failureOperationRoomCreate    failureOperationType = "room_create"
)

var failureOperationTypeRegistry = map[failureOperationType]struct{}{
	failureOperationMessageCreate: {}, failureOperationMemberAdd: {},
	failureOperationMemberRemove: {}, failureOperationRoomRename: {},
	failureOperationMuteToggle: {}, failureOperationRoomCreate: {},
}
```

Extend the effect constants (`:60-64`):

```go
const (
	failureEffectAdmission        failureEffect = "admission"
	failureEffectMessagePersisted failureEffect = "message_persisted"
	failureEffectRecipientEvent   failureEffect = "recipient_event"
	failureEffectMemberState      failureEffect = "member_state"
	failureEffectRoomName         failureEffect = "room_name"
	failureEffectSubscriptionMute failureEffect = "subscription_mute"
	failureEffectRoomCreated      failureEffect = "room_created"
)
```

Extend the reason constants (`:114-124`) and `failureReasonRegistry` (`:126`) with the four new reasons from "Shared vocabulary".

Extend the lane registry (`:166`):

```go
var failureOperationLaneRegistry = map[string]struct{}{
	soakFailureLaneMessageSend:    {},
	soakFailureLaneMemberMutation: {},
	soakFailureLaneRoomMutation:   {},
	soakFailureLaneRoomCreate:     {},
}
```

- [ ] **Step 4: Make the operation-type check registry-driven**

In `validateFailureOperation` (`failure_ledger.go:1229`), replace the hard-coded comparison:

```go
		if _, known := failureOperationTypeRegistry[operation.OperationType]; !known {
			return fmt.Errorf("version 2 failure operation %q has unsupported type %q", operation.ID, operation.OperationType)
		}
		if operation.RunID == "" || len(operation.Targets) == 0 || len(operation.Effects) == 0 {
			return fmt.Errorf("version 2 failure operation %q requires run, type, targets, and effects", operation.ID)
		}
```

- [ ] **Step 5: Make the contract per-lane**

In `failure_ledger.go`, bump the version and add `Lanes`:

```go
const failureObserverContractSchemaVersion = 2

type failureObserverContract struct {
	SchemaVersion            int                          `json:"schemaVersion"`
	Scenario                 string                       `json:"scenario"`
	Observers                []failureObserver            `json:"observers"`
	Lanes                    map[string][]failureObserver `json:"lanes"`
	RecipientObserverEnabled bool                         `json:"recipientObserverEnabled"`
}

func newFailureObserverContract(recipientEnabled bool) failureObserverContract {
	messageObservers := []failureObserver{failureObserverAdmission, failureObserverHistory}
	if recipientEnabled {
		messageObservers = append(messageObservers, failureObserverRecipient)
	}
	roomObservers := []failureObserver{failureObserverAdmission, failureObserverRoomState}
	lanes := map[string][]failureObserver{
		soakFailureLaneMessageSend:    messageObservers,
		soakFailureLaneMemberMutation: append([]failureObserver(nil), roomObservers...),
		soakFailureLaneRoomMutation:   append([]failureObserver(nil), roomObservers...),
		soakFailureLaneRoomCreate:     append([]failureObserver(nil), roomObservers...),
	}
	observers := []failureObserver{failureObserverAdmission, failureObserverHistory, failureObserverRoomState}
	if recipientEnabled {
		observers = append(observers, failureObserverRecipient)
	}
	slices.Sort(observers)
	return failureObserverContract{
		SchemaVersion: failureObserverContractSchemaVersion,
		Scenario:      soakFailureScenario,
		Observers:     observers, Lanes: lanes,
		RecipientObserverEnabled: recipientEnabled,
	}
}
```

Update `validateFailureObserverContract` (`:1581`) to also validate every lane:

```go
	if len(contract.Lanes) == 0 {
		return fmt.Errorf("observer contract must declare at least one lane")
	}
	for lane, observers := range contract.Lanes {
		if _, known := failureOperationLaneRegistry[lane]; !known {
			return fmt.Errorf("observer contract declares unsupported lane %q", lane)
		}
		if len(observers) == 0 {
			return fmt.Errorf("observer contract lane %q declares no observer", lane)
		}
		if err := validateRegisteredObservers(observers); err != nil {
			return fmt.Errorf("observer contract lane %q: %w", lane, err)
		}
	}
```

Keep the existing recipient-enablement check but scope it to the message lane:

```go
	hasRecipient := slices.Contains(contract.Lanes[soakFailureLaneMessageSend], failureObserverRecipient)
	if hasRecipient != contract.RecipientObserverEnabled {
		return fmt.Errorf("recipient observer enablement does not match configured observers")
	}
```

Update `equalFailureObserverContract` (`:1598`) to compare `Lanes` with `maps.EqualFunc(left.Lanes, right.Lanes, slices.Equal)`, `cloneFailureObserverContract` (`:1605`) to deep-copy `Lanes`, and `failureOperationMatchesObserverContract` (`:1614`) to compare against the lane's set:

```go
func failureOperationMatchesObserverContract(
	operation *failureOperation,
	contract failureObserverContract,
) bool {
	if operation == nil || operation.Scenario != contract.Scenario {
		return false
	}
	configured, known := contract.Lanes[operation.Lane]
	if !known {
		return false
	}
	expected := append([]failureObserver(nil), operation.Expected...)
	slices.Sort(expected)
	laneObservers := append([]failureObserver(nil), configured...)
	slices.Sort(laneObservers)
	return slices.Equal(expected, laneObservers)
}
```

- [ ] **Step 6: Add the WAL epoch path helper and the per-lane effect builders**

In `tools/loadgen/soak_failure.go`:

```go
const soakFailureDefaultLedgerEpoch = "v1"

// failureWALPath separates the two identities the journal used to conflate:
// runID owns the seeded topology, epoch owns the evidence journal. Bumping the
// epoch starts a fresh journal without re-seeding 10k rooms.
func failureWALPath(dir, runID, epoch string) string {
	if epoch == "" {
		epoch = soakFailureDefaultLedgerEpoch
	}
	return filepath.Join(dir, runID+"."+epoch+".wal")
}
```

In `failure_ledger.go`, next to `messageCreateExpectedEffectsForObservers` (`:83`):

```go
func memberMutationExpectedEffects() []failureExpectedEffect {
	return []failureExpectedEffect{
		{Effect: failureEffectAdmission, Observer: failureObserverAdmission, Required: true},
		{Effect: failureEffectMemberState, Observer: failureObserverRoomState, Required: true},
	}
}

func roomMutationExpectedEffects(operationType failureOperationType) []failureExpectedEffect {
	effect := failureEffectRoomName
	if operationType == failureOperationMuteToggle {
		effect = failureEffectSubscriptionMute
	}
	return []failureExpectedEffect{
		{Effect: failureEffectAdmission, Observer: failureObserverAdmission, Required: true},
		{Effect: effect, Observer: failureObserverRoomState, Required: true},
	}
}

func roomCreateExpectedEffects() []failureExpectedEffect {
	return []failureExpectedEffect{
		{Effect: failureEffectAdmission, Observer: failureObserverAdmission, Required: true},
		{Effect: failureEffectRoomCreated, Observer: failureObserverRoomState, Required: true},
	}
}
```

- [ ] **Step 7: Register the `room_state` observer**

In `tools/loadgen/failure_observer.go`:

```go
const failureObserverRoomState failureObserver = "room_state"
```

and add to `failureObserverRegistry`:

```go
	failureObserverRoomState: {
		Name: failureObserverRoomState, Mode: failureObserverQuery,
		Effects: []failureEffect{
			failureEffectMemberState, failureEffectRoomName,
			failureEffectSubscriptionMute, failureEffectRoomCreated,
		},
		FinalReconciliation: true,
	},
```

- [ ] **Step 8: Point the ledger opener at the epoch path**

In `tools/loadgen/soak_failure.go` `openSoakFailureLedger` (`:84`), replace the path construction and add the abandoned-journal count:

```go
	if cfg.LedgerDir != "" {
		wal, err := openFailureWAL(failureWALPath(cfg.LedgerDir, cfg.RunID, cfg.LedgerEpoch))
		if err != nil {
			return nil, fmt.Errorf(
				"open soak failure WAL for run %q epoch %q: %w",
				cfg.RunID, cfg.LedgerEpoch, err,
			)
		}
		recordAbandonedFailureJournals(metrics, cfg.LedgerDir, cfg.RunID, cfg.LedgerEpoch)
		...
```

and:

```go
// recordAbandonedFailureJournals counts retained journals from earlier epochs
// of the same run. They are evidence and stay on disk, but they belong to an
// incompatible contract and are never replayed, so the epoch boundary must be
// visible rather than silent.
func recordAbandonedFailureJournals(metrics *Metrics, dir, runID, epoch string) {
	if metrics == nil {
		return
	}
	active := failureWALPath(dir, runID, epoch)
	matches, err := filepath.Glob(filepath.Join(dir, runID+"*.wal"))
	if err != nil {
		slog.Error("scan retained failure journals", "error", err)
		return
	}
	abandoned := 0
	for _, match := range matches {
		if match != active {
			abandoned++
		}
	}
	metrics.FailureAbandonedJournals.Set(float64(abandoned))
	if abandoned > 0 {
		slog.Warn("retained failure journals from earlier epochs are not replayed",
			"runId", runID, "epoch", epoch, "abandoned", abandoned)
	}
}
```

`Metrics.FailureAbandonedJournals` is added in Task 4; until then this step will not compile — add the gauge in `metrics.go` now as part of this step:

```go
	m.FailureAbandonedJournals = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "loadgen_failure_abandoned_journals",
			Help: "Retained failure journals for this run ID that belong to an earlier ledger epoch and are not replayed.",
		},
	)
```

register it in `r.MustRegister(...)` and declare the field on the `Metrics` struct.

- [ ] **Step 9: Add `LedgerEpoch` to the config struct**

In `tools/loadgen/soak_config.go`, add to `soakConfig` next to `LedgerDir` (`:63`):

```go
	LedgerEpoch                  string        `env:"LEDGER_EPOCH"                     envDefault:"v1"`
```

and in `validateSoakConfig`, after the `LedgerCapacity` check:

```go
	if !failureRunIDPattern.MatchString(cfg.LedgerEpoch) ||
		cfg.LedgerEpoch == "." || cfg.LedgerEpoch == ".." {
		return fmt.Errorf("SOAK_LEDGER_EPOCH must be a filename-safe identifier")
	}
```

- [ ] **Step 10: Run the tests**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS. Existing WAL tests still pass because the WAL record schema is unchanged — only the embedded contract changed.

- [ ] **Step 11: Run the scoped failure coverage gate**

Run: `make coverage-loadgen-failure`
Expected: PASS at the 80/90 thresholds.

- [ ] **Step 12: Commit**

```bash
git add tools/loadgen/failure_ledger.go tools/loadgen/failure_observer.go \
        tools/loadgen/failure_ledger_test.go tools/loadgen/soak_failure.go \
        tools/loadgen/soak_config.go tools/loadgen/metrics.go
git commit -m "feat(loadgen): give the failure ledger per-lane observer contracts and a journal epoch"
```

---

### Task 2: Bounded RPC actions, wire carriers, and metric families

**Files:**
- Modify: `tools/loadgen/soak_rpc.go:18-46`
- Modify: `tools/loadgen/soak_wire.go`
- Modify: `tools/loadgen/metrics.go`
- Test: `tools/loadgen/soak_rpc_test.go`, `tools/loadgen/soak_wire_test.go`, `tools/loadgen/metrics_test.go`

**Interfaces:**
- Consumes: Task 1's constants.
- Produces:
  - `soakRPCMemberAdd`, `soakRPCMemberRemove`, `soakRPCRoomRename`, `soakRPCMuteToggle`, `soakRPCRoomCreate`, `soakRPCMemberList`, `soakRPCRoomsInfo`, `soakRPCSubscriptionList`, `soakRPCRoomStateRead`
  - `soakAddMembersRequest`, `soakRemoveMemberRequest`, `soakRoomRenameRequest`, `soakCreateRoomRequest`, `soakStatusReply`, `soakCreateRoomReply`, `soakMuteToggleReply`, `soakListMembersResponse`, `soakRoomsInfoRequest`, `soakRoomsInfoResponse`, `soakSubscriptionListResponse`
  - `Metrics.SoakRoomCandidates`, `Metrics.SoakRoomQuarantineProbes`, `Metrics.SoakRoomPoolExhausted`, `Metrics.SoakRoomPoolDegraded`, `Metrics.SoakRoomCreateBudgetRemaining`, `Metrics.SoakRoomStateSources`

- [ ] **Step 1: Write the failing tests**

Add to `tools/loadgen/soak_rpc_test.go`:

```go
func TestValidSoakRPCAction_AcceptsRoomAndMemberActions(t *testing.T) {
	for _, action := range []soakRPCAction{
		soakRPCMemberAdd, soakRPCMemberRemove, soakRPCRoomRename, soakRPCMuteToggle,
		soakRPCRoomCreate, soakRPCMemberList, soakRPCRoomsInfo,
		soakRPCSubscriptionList, soakRPCRoomStateRead,
	} {
		t.Run(string(action), func(t *testing.T) {
			assert.True(t, validSoakRPCAction(action))
		})
	}
}
```

Add to `tools/loadgen/soak_wire_test.go`:

```go
func TestSoakAddMembersRequest_MatchesModelContract(t *testing.T) {
	encoded, err := json.Marshal(soakAddMembersRequest{RoomID: "r1", Users: []string{"u1"}})
	require.NoError(t, err)

	var decoded model.AddMembersRequest
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, "r1", decoded.RoomID)
	assert.Equal(t, []string{"u1"}, decoded.Users)
}

func TestSoakCreateRoomReply_DecodesRoomServiceReply(t *testing.T) {
	encoded, err := json.Marshal(model.CreateRoomReply{
		Status: model.CreateRoomReplyAccepted, RoomID: "room-1", RoomType: string(model.RoomTypeChannel),
	})
	require.NoError(t, err)

	var decoded soakCreateRoomReply
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, "accepted", decoded.Status)
	assert.Equal(t, "room-1", decoded.RoomID)
}

func TestSoakListMembersResponse_DecodesRoomServiceReply(t *testing.T) {
	encoded, err := json.Marshal(model.ListRoomMembersResponse{
		Members: []model.RoomMember{{ID: "m1", RoomID: "r1"}},
	})
	require.NoError(t, err)

	var decoded soakListMembersResponse
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Len(t, decoded.Members, 1)
	assert.Equal(t, "r1", decoded.Members[0].RoomID)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL — `undefined: soakRPCMemberAdd`, `undefined: soakAddMembersRequest`.

- [ ] **Step 3: Add the actions**

In `tools/loadgen/soak_rpc.go`, append the nine constants to the `const` block (`:18`) and to the `switch` in `validSoakRPCAction` (`:36`).

- [ ] **Step 4: Add the wire carriers**

Append to `tools/loadgen/soak_wire.go`:

```go
// Room and member request/reply carriers. They mirror pkg/model, but stay
// local so a loadgen change can never widen a production struct; the contract
// tests in soak_wire_test.go marshal against the real model types.
type soakAddMembersRequest struct {
	RoomID string   `json:"roomId"`
	Users  []string `json:"users"`
}

type soakRemoveMemberRequest struct {
	RoomID  string `json:"roomId"`
	Account string `json:"account"`
}

type soakRoomRenameRequest struct {
	NewName string `json:"newName"`
}

type soakCreateRoomRequest struct {
	Name  string   `json:"name"`
	Users []string `json:"users"`
}

type soakStatusReply struct {
	Status    string `json:"status"`
	RequestID string `json:"requestId,omitempty"`
}

type soakCreateRoomReply struct {
	Status   string `json:"status"`
	RoomID   string `json:"roomId"`
	RoomType string `json:"roomType"`
}

type soakMuteToggleReply struct {
	Status string `json:"status"`
	Muted  bool   `json:"muted"`
}

type soakRoomMember struct {
	ID     string `json:"id"`
	RoomID string `json:"rid"`
	Member struct {
		Type    string `json:"type"`
		ID      string `json:"id"`
		Account string `json:"account"`
	} `json:"member"`
}

type soakListMembersResponse struct {
	Members []soakRoomMember `json:"members"`
}

type soakRoomsInfoRequest struct {
	RoomIDs []string `json:"roomIds"`
}

type soakRoomsInfoResponse struct {
	Rooms []struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		UserCount int    `json:"userCount"`
	} `json:"rooms"`
}

type soakSubscriptionListResponse struct {
	Subscriptions []struct {
		RoomID string `json:"roomId"`
		Muted  bool   `json:"muted"`
	} `json:"subscriptions"`
}
```

Before writing the member/rooms-info/subscription-list field names, open `pkg/model/member.go:53-138` and `room-service/handler.go:410` (`listMembers`) and `:1160` (`roomsInfoBatch`), plus `user-service`'s subscription-list reply type, and copy the exact JSON tags. If a field name differs from the sketch above, the model is authoritative — fix the carrier, not the model.

- [ ] **Step 5: Add the metric families**

In `tools/loadgen/metrics.go`, declare the fields and construct them:

```go
	m.SoakRoomCandidates = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "loadgen_soak_room_candidates", Help: "Member candidates by bounded lifecycle state."},
		[]string{"state"},
	)
	m.SoakRoomQuarantineProbes = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "loadgen_soak_room_quarantine_probes_total", Help: "Quarantined member candidate re-probes by bounded result."},
		[]string{"result"},
	)
	m.SoakRoomPoolExhausted = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "loadgen_soak_room_pool_exhausted_total", Help: "Member mutations skipped because a room had no usable candidate, by bounded reason."},
		[]string{"reason"},
	)
	m.SoakRoomPoolDegraded = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "loadgen_soak_room_pool_degraded", Help: "Whether the member candidate pool is currently degraded. Reversible: it clears when the pool recovers."},
	)
	m.SoakRoomCreateBudgetRemaining = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "loadgen_soak_room_create_budget_remaining", Help: "Remaining room-create budget for this run."},
	)
	m.SoakRoomStateSources = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "loadgen_soak_room_state_source_total", Help: "Room-state observer source outcomes by bounded source and result."},
		[]string{"source", "result"},
	)
```

Register all six plus `FailureAbandonedJournals` in `r.MustRegister(...)`.

- [ ] **Step 6: Assert the label sets are bounded**

Add to `tools/loadgen/metrics_test.go`:

```go
func TestNewMetrics_RoomLaneFamiliesUseBoundedLabels(t *testing.T) {
	metrics := NewMetrics()
	metrics.SoakRoomCandidates.WithLabelValues("available").Set(1)
	metrics.SoakRoomQuarantineProbes.WithLabelValues("resolved").Inc()
	metrics.SoakRoomPoolExhausted.WithLabelValues("quarantine_full").Inc()
	metrics.SoakRoomStateSources.WithLabelValues("room_service", "good").Inc()

	families, err := metrics.Registry.Gather()
	require.NoError(t, err)
	names := make(map[string]struct{}, len(families))
	for _, family := range families {
		names[family.GetName()] = struct{}{}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				assert.NotContains(t, []string{"room_id", "account", "message_id", "run_id", "operation_id"},
					label.GetName(), "family %s must not carry high-cardinality labels", family.GetName())
			}
		}
	}
	for _, name := range []string{
		"loadgen_soak_room_candidates", "loadgen_soak_room_quarantine_probes_total",
		"loadgen_soak_room_pool_exhausted_total", "loadgen_soak_room_pool_degraded",
		"loadgen_soak_room_create_budget_remaining", "loadgen_soak_room_state_source_total",
		"loadgen_failure_abandoned_journals",
	} {
		assert.Contains(t, names, name)
	}
}
```

- [ ] **Step 7: Run the tests**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add tools/loadgen/soak_rpc.go tools/loadgen/soak_rpc_test.go \
        tools/loadgen/soak_wire.go tools/loadgen/soak_wire_test.go \
        tools/loadgen/metrics.go tools/loadgen/metrics_test.go
git commit -m "feat(loadgen): add bounded room/member RPC actions, wire carriers, and metrics"
```

---

### Task 3: Candidate ring, leases, and quarantine (`soak_roomstate.go`)

**Files:**
- Create: `tools/loadgen/soak_roomstate.go`
- Test: `tools/loadgen/soak_roomstate_test.go`

**Interfaces:**
- Consumes: `soakTopology` (`soak_topology.go:14`), `Metrics` from Task 2.
- Produces:

```go
type soakMemberCandidateState string

const (
	soakMemberCandidateAvailable   soakMemberCandidateState = "available"
	soakMemberCandidateAddInflight soakMemberCandidateState = "add_inflight"
	soakMemberCandidateMember      soakMemberCandidateState = "member"
	soakMemberCandidateRemoveIn    soakMemberCandidateState = "remove_inflight"
	soakMemberCandidateQuarantined soakMemberCandidateState = "quarantined"
)

type soakMemberIntent struct {
	RoomID    string
	Account   string
	Requester string
	Add       bool
}

type soakRoomStatePool struct{ /* unexported */ }

func newSoakRoomStatePool(topology *soakTopology, quarantineMax int, metrics *Metrics, rng *rand.Rand) (*soakRoomStatePool, error)
func (p *soakRoomStatePool) NextMemberIntent() (soakMemberIntent, bool)
func (p *soakRoomStatePool) SettleMember(intent soakMemberIntent, result failureResult)
func (p *soakRoomStatePool) NextQuarantineProbe() (soakMemberIntent, bool)
func (p *soakRoomStatePool) ResolveQuarantine(roomID, account string, isMember bool)
func (p *soakRoomStatePool) ReleaseQuarantine(roomID, account string)
func (p *soakRoomStatePool) NextRenameIntent() (roomID, requester, newName string, ok bool)
func (p *soakRoomStatePool) SettleRename(roomID, name string, result failureResult)
func (p *soakRoomStatePool) NextMuteIntent() (roomID, account string, targetMuted bool, ok bool)
func (p *soakRoomStatePool) SettleMute(roomID, account string, targetMuted bool, result failureResult)
func (p *soakRoomStatePool) RoomIDs() []string
```

Behavioural contract:

- `NextMemberIntent` round-robins rooms. It returns an `add` intent for an `available` candidate, or a `remove` intent for a `member` candidate, preferring `member` so add/remove stays paired and room size oscillates by one. It takes a `(room, account)` lease and a per-room single-flight lease; a room that already has a member mutation in flight is skipped.
- `SettleMember` releases both leases and moves the candidate: `good` → `member` (after add) or `available` (after remove); `bad` → back to the pre-intent state (room-service rejected it, so nothing changed); `unverified`/`missing_after_deadline` → `quarantined`; `not_sent` → back to the pre-intent state.
- The quarantine is a bounded FIFO. When it is full, the candidate is dropped from the room's ring, `SoakRoomPoolExhausted{reason="quarantine_full"}` increments, and `SoakRoomPoolDegraded` is set to 1. It returns to 0 once the quarantine drops back below its high-water mark and every room has at least one usable candidate.
- Rename and mute intents are serialized per room and per `(room, account)` respectively, with the same lease mechanism. Mute tracks the last known state so the expected post-toggle value is always `!lastKnown`; a settle other than `good` marks the `(room, account)` mute state unknown and the pair is quarantined for a state re-probe rather than toggled again.
- Only channel rooms are eligible for member mutations and renames (room-service rejects DM/botDM). Mute applies to any room type.
- The room owner (the first subscription of a channel, `soak_topology.go:324`) is the requester and is never a removal target.

- [ ] **Step 1: Write the failing tests**

Create `tools/loadgen/soak_roomstate_test.go`:

```go
package main

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func soakRoomStateTestTopology() *soakTopology {
	users := []model.User{
		{ID: "u1", Account: "user-1"}, {ID: "u2", Account: "user-2"},
		{ID: "u3", Account: "user-3"}, {ID: "u4", Account: "user-4"},
	}
	return &soakTopology{
		BorrowedUsers: users,
		ActiveUsers:   users[:2],
		Rooms:         []model.Room{{ID: "room-1", Type: model.RoomTypeChannel, Name: "soak-channel"}},
		Subscriptions: []model.Subscription{
			{RoomID: "room-1", User: model.SubscriptionUser{ID: "u1", Account: "user-1"},
				Roles: []model.Role{model.RoleOwner}, IsSubscribed: true, RoomType: model.RoomTypeChannel},
			{RoomID: "room-1", User: model.SubscriptionUser{ID: "u2", Account: "user-2"},
				Roles: []model.Role{model.RoleMember}, IsSubscribed: true, RoomType: model.RoomTypeChannel},
		},
	}
}

func TestSoakRoomStatePool_CyclesCandidatesForever(t *testing.T) {
	pool, err := newSoakRoomStatePool(soakRoomStateTestTopology(), 4, NewMetrics(), rand.New(rand.NewSource(1)))
	require.NoError(t, err)

	seen := make(map[string]int)
	for range 20 {
		intent, ok := pool.NextMemberIntent()
		require.True(t, ok, "the ring must never run dry when every cycle completes")
		assert.Equal(t, "user-1", intent.Requester)
		assert.NotEqual(t, "user-1", intent.Account, "the owner is never a mutation target")
		seen[intent.Account]++
		pool.SettleMember(intent, failureResultGood)
	}
	assert.GreaterOrEqual(t, len(seen), 2, "candidates must be reused, not consumed")
}

func TestSoakRoomStatePool_PairsAddThenRemove(t *testing.T) {
	pool, err := newSoakRoomStatePool(soakRoomStateTestTopology(), 4, NewMetrics(), rand.New(rand.NewSource(2)))
	require.NoError(t, err)

	first, ok := pool.NextMemberIntent()
	require.True(t, ok)
	assert.True(t, first.Add)
	pool.SettleMember(first, failureResultGood)

	second, ok := pool.NextMemberIntent()
	require.True(t, ok)
	assert.False(t, second.Add, "a verified member must be removed before the candidate is reused")
	assert.Equal(t, first.Account, second.Account)
}

func TestSoakRoomStatePool_LeasesPreventConflictingOperations(t *testing.T) {
	pool, err := newSoakRoomStatePool(soakRoomStateTestTopology(), 4, NewMetrics(), rand.New(rand.NewSource(3)))
	require.NoError(t, err)

	first, ok := pool.NextMemberIntent()
	require.True(t, ok)

	_, ok = pool.NextMemberIntent()
	assert.False(t, ok, "a room with an in-flight member mutation must not issue another")

	pool.SettleMember(first, failureResultGood)
	_, ok = pool.NextMemberIntent()
	assert.True(t, ok)
}

func TestSoakRoomStatePool_UnverifiedResultQuarantinesCandidate(t *testing.T) {
	metrics := NewMetrics()
	pool, err := newSoakRoomStatePool(soakRoomStateTestTopology(), 4, metrics, rand.New(rand.NewSource(4)))
	require.NoError(t, err)

	intent, ok := pool.NextMemberIntent()
	require.True(t, ok)
	pool.SettleMember(intent, failureResultUnverified)

	probe, ok := pool.NextQuarantineProbe()
	require.True(t, ok)
	assert.Equal(t, intent.Account, probe.Account)

	pool.ResolveQuarantine(probe.RoomID, probe.Account, true)
	next, ok := pool.NextMemberIntent()
	require.True(t, ok)
	assert.Equal(t, probe.Account, next.Account)
	assert.False(t, next.Add, "a candidate confirmed present must be removed, not re-added")
}

func TestSoakRoomStatePool_QuarantineOverflowDegradesWithoutInvalidation(t *testing.T) {
	metrics := NewMetrics()
	pool, err := newSoakRoomStatePool(soakRoomStateTestTopology(), 1, metrics, rand.New(rand.NewSource(5)))
	require.NoError(t, err)

	for range 2 {
		intent, ok := pool.NextMemberIntent()
		if !ok {
			break
		}
		pool.SettleMember(intent, failureResultUnverified)
	}

	assert.Equal(t, 1.0, testutilGaugeValue(t, metrics.SoakRoomPoolDegraded))
}

func TestSoakRoomStatePool_MuteIntentAlternatesFromLastKnownState(t *testing.T) {
	pool, err := newSoakRoomStatePool(soakRoomStateTestTopology(), 4, NewMetrics(), rand.New(rand.NewSource(6)))
	require.NoError(t, err)

	roomID, account, target, ok := pool.NextMuteIntent()
	require.True(t, ok)
	assert.True(t, target, "the seeded subscription is unmuted, so the first toggle must mute")
	pool.SettleMute(roomID, account, target, failureResultGood)

	_, _, nextTarget, ok := pool.NextMuteIntent()
	require.True(t, ok)
	assert.False(t, nextTarget)
}

func TestSoakRoomStatePool_AmbiguousMuteIsNotToggledAgain(t *testing.T) {
	pool, err := newSoakRoomStatePool(soakRoomStateTestTopology(), 4, NewMetrics(), rand.New(rand.NewSource(7)))
	require.NoError(t, err)

	roomID, account, target, ok := pool.NextMuteIntent()
	require.True(t, ok)
	pool.SettleMute(roomID, account, target, failureResultUnverified)

	_, _, _, ok = pool.NextMuteIntent()
	assert.False(t, ok, "an unknown mute state must be re-probed, never re-toggled")
}

func TestSoakRoomStatePool_RenameProducesDistinctNames(t *testing.T) {
	pool, err := newSoakRoomStatePool(soakRoomStateTestTopology(), 4, NewMetrics(), rand.New(rand.NewSource(8)))
	require.NoError(t, err)

	roomID, requester, first, ok := pool.NextRenameIntent()
	require.True(t, ok)
	assert.Equal(t, "user-1", requester)
	pool.SettleRename(roomID, first, failureResultGood)

	_, _, second, ok := pool.NextRenameIntent()
	require.True(t, ok)
	assert.NotEqual(t, first, second)
}

func TestSoakRoomStatePool_RejectsTopologyWithoutChannelRooms(t *testing.T) {
	_, err := newSoakRoomStatePool(&soakTopology{
		Rooms: []model.Room{{ID: "dm-1", Type: model.RoomTypeDM}},
	}, 4, NewMetrics(), rand.New(rand.NewSource(9)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "channel room")
}
```

Add the gauge helper at the bottom of the same file:

```go
func testutilGaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	t.Helper()
	var metric dto.Metric
	require.NoError(t, gauge.Write(&metric))
	return metric.GetGauge().GetValue()
}
```

with imports `"github.com/prometheus/client_golang/prometheus"` and `dto "github.com/prometheus/client_model/go"`.

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL — `undefined: newSoakRoomStatePool`.

- [ ] **Step 3: Implement the pool**

Create `tools/loadgen/soak_roomstate.go`. Structure:

```go
package main

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/hmchangw/chat/pkg/model"
)

type soakRoomStatePool struct {
	mu         sync.Mutex
	rooms      []*soakRoomState
	roomsByID  map[string]*soakRoomState
	cursor     int
	quarantine []soakMemberIntent
	quarantineMax int
	metrics    *Metrics
	rng        *rand.Rand
}

type soakRoomState struct {
	id           string
	name         string
	renameSeq    int
	owner        string
	members      []string                  // seeded members, never mutated by the lane
	candidates   []string                  // ring of reusable non-member accounts
	states       map[string]soakMemberCandidateState
	mute         map[string]soakMuteState
	memberLease  bool
	renameLease  bool
	muteLeases   map[string]struct{}
}

type soakMuteState struct {
	muted   bool
	known   bool
}
```

Key implementation notes for the engineer:

1. `newSoakRoomStatePool` walks `topology.Subscriptions`, groups by room, keeps only `model.RoomTypeChannel` rooms with an owner (`hasRole(..., model.RoleOwner)` equivalent: the subscription whose `Roles` contains `model.RoleOwner`), and builds the candidate ring as `topology.BorrowedUsers` minus that room's subscribed accounts, minus bots. Return an error when no channel room qualifies (`fmt.Errorf("soak room state pool requires at least one channel room with an owner")`).
2. Cap the per-room ring to at most 64 candidates. A ring larger than that buys nothing (the lane runs a handful of mutations per second) and keeping every one of 20k borrowed users per room would cost hundreds of megabytes across 3,000 rooms.
3. `NextMemberIntent` scans at most `len(rooms)` rooms from `cursor`. It skips rooms whose `memberLease` is held. Within a room, prefer a candidate in state `member` (issue a remove) over one in `available` (issue an add). Take the lease before returning.
4. `SettleMember` looks up the room, releases the lease, and applies the transition table from the behavioural contract above. Recompute the candidate gauge after each settle with a single `refreshGaugesLocked()` that sets `SoakRoomCandidates` for all five states.
5. Quarantine is a slice used as a FIFO. `NextQuarantineProbe` pops the front and returns it without a lease (the probe is read-only). `ResolveQuarantine` places the candidate into `member` or `available`; `ReleaseQuarantine` pushes it back to the tail when the probe could not answer.
6. `SoakRoomPoolDegraded` is set to 1 when the quarantine is full or a room's ring is empty, and reset to 0 in `refreshGaugesLocked` when neither holds. It is never accompanied by a ledger invalidation.
7. Rename names are `fmt.Sprintf("%s-r%d", state.baseName, state.renameSeq)` where `baseName` is the seeded room name truncated to 80 runes (room-service caps names at 100 runes, `room-service/handler.go:1929`).
8. Mute: `NextMuteIntent` picks a room round-robin, then an account from the room's *seeded* members (never a lane-managed candidate, so mute and membership can never contend for the same account), skips it when `mute[account].known == false` or a lease is held, and returns `!mute[account].muted`.

- [ ] **Step 4: Run the tests**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/soak_roomstate.go tools/loadgen/soak_roomstate_test.go
git commit -m "feat(loadgen): add a reusable room/member candidate pool with leases and quarantine"
```

---

### Task 4: Room mutation RPCs (`soak_roomops.go`)

**Files:**
- Create: `tools/loadgen/soak_roomops.go`
- Test: `tools/loadgen/soak_roomops_test.go`

**Interfaces:**
- Consumes: `soakRPCClient` (`soak_rpc.go:209`), Task 2's actions and carriers.
- Produces:

```go
type soakRoomMutationOutcome struct {
	Action     soakRPCAction
	Latency    time.Duration
	Retries    int
	ErrorClass soakErrorClass
	Accepted   bool
	RoomID     string
	Muted      bool
}

type soakRoomMutator struct{ /* unexported */ }

func newSoakRoomMutator(siteID string, rpc *soakRPCClient, timeout time.Duration, now func() time.Time) *soakRoomMutator
func (m *soakRoomMutator) AddMember(ctx context.Context, requester, roomID, account string) (soakRoomMutationOutcome, error)
func (m *soakRoomMutator) RemoveMember(ctx context.Context, requester, roomID, account string) (soakRoomMutationOutcome, error)
func (m *soakRoomMutator) Rename(ctx context.Context, requester, roomID, newName string) (soakRoomMutationOutcome, error)
func (m *soakRoomMutator) ToggleMute(ctx context.Context, account, roomID string) (soakRoomMutationOutcome, error)
func (m *soakRoomMutator) CreateRoom(ctx context.Context, requester, name string, users []string) (soakRoomMutationOutcome, error)
```

**Critical:** every call uses `RetryMode: soakRetryNever`. These are non-idempotent mutations tracked by the ledger; a transport-level retry would double-apply a remove or reverse a mute toggle. Ambiguity is resolved by the ledger's reconciliation, never by the RPC client.

- [ ] **Step 1: Write the failing tests**

Create `tools/loadgen/soak_roomops_test.go` with a fake transport:

```go
type soakRoomOpsTransport struct {
	subjects []string
	bodies   [][]byte
	reply    []byte
	err      error
}

func (t *soakRoomOpsTransport) Request(_ context.Context, subject string, data []byte, _ time.Duration) ([]byte, error) {
	t.subjects = append(t.subjects, subject)
	t.bodies = append(t.bodies, append([]byte(nil), data...))
	return t.reply, t.err
}

func newSoakRoomOpsMutator(transport soakRPCTransport) *soakRoomMutator {
	return newSoakRoomMutator("site-a", newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 3}, nil, nil), time.Second, nil)
}

func TestSoakRoomMutator_AddMemberUsesOwnerSubject(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"status":"accepted"}`)}

	outcome, err := newSoakRoomOpsMutator(transport).AddMember(context.Background(), "user-1", "room-1", "user-9")

	require.NoError(t, err)
	assert.True(t, outcome.Accepted)
	assert.Equal(t, soakRPCMemberAdd, outcome.Action)
	assert.Equal(t, subject.MemberAdd("user-1", "room-1", "site-a"), transport.subjects[0])
	assert.JSONEq(t, `{"roomId":"room-1","users":["user-9"]}`, string(transport.bodies[0]))
}

func TestSoakRoomMutator_MutationsNeverRetry(t *testing.T) {
	transport := &soakRoomOpsTransport{err: nats.ErrTimeout}

	_, err := newSoakRoomOpsMutator(transport).RemoveMember(context.Background(), "user-1", "room-1", "user-9")

	require.Error(t, err)
	assert.Len(t, transport.subjects, 1, "an ambiguous mutation must never be retried on the wire")
}

func TestSoakRoomMutator_ToggleMuteReturnsObservedState(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"status":"ok","muted":true}`)}

	outcome, err := newSoakRoomOpsMutator(transport).ToggleMute(context.Background(), "user-2", "room-1")

	require.NoError(t, err)
	assert.True(t, outcome.Muted)
	assert.Equal(t, subject.MuteToggle("user-2", "room-1", "site-a"), transport.subjects[0])
}

func TestSoakRoomMutator_CreateRoomReturnsRoomID(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"status":"accepted","roomId":"room-new","roomType":"channel"}`)}

	outcome, err := newSoakRoomOpsMutator(transport).CreateRoom(context.Background(), "user-1", "soak-room", []string{"user-2"})

	require.NoError(t, err)
	assert.Equal(t, "room-new", outcome.RoomID)
	assert.True(t, outcome.Accepted)
}

func TestSoakRoomMutator_ClassifiesRejection(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"error":{"code":"forbidden","message":"only owners"}}`)}

	outcome, err := newSoakRoomOpsMutator(transport).Rename(context.Background(), "user-2", "room-1", "soak-room-r1")

	require.Error(t, err)
	assert.False(t, outcome.Accepted)
	assert.Equal(t, soakErrorForbidden, outcome.ErrorClass)
}
```

Before running, confirm the error envelope shape by reading `pkg/errcode`'s marshaller and an existing test such as `tools/loadgen/soak_rpc_test.go`; use the exact envelope those tests use.

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL — `undefined: newSoakRoomMutator`.

- [ ] **Step 3: Implement the mutator**

Create `tools/loadgen/soak_roomops.go`. Each method follows this shape (shown for `AddMember`):

```go
func (m *soakRoomMutator) AddMember(
	ctx context.Context,
	requester, roomID, account string,
) (soakRoomMutationOutcome, error) {
	var reply soakStatusReply
	return m.call(ctx, soakRoomMutationOutcome{Action: soakRPCMemberAdd, RoomID: roomID}, soakRPCRequest{
		Action:  soakRPCMemberAdd,
		Subject: subject.MemberAdd(requester, roomID, m.siteID),
		Body:    soakAddMembersRequest{RoomID: roomID, Users: []string{account}},
		Timeout: m.timeout, RetryMode: soakRetryNever,
	}, &reply, func(outcome *soakRoomMutationOutcome) {
		outcome.Accepted = reply.Status == "accepted"
	})
}
```

with a shared `call` helper that times the request, copies `result.Retries`/`result.ErrorClass` into the outcome, and runs the closure only on success. Subjects: `subject.MemberAdd`, `subject.MemberRemove`, `subject.RoomRename`, `subject.MuteToggle`, `subject.RoomCreate` (`pkg/subject/subject.go:826,198,186,864,931`).

- [ ] **Step 4: Run the tests**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/soak_roomops.go tools/loadgen/soak_roomops_test.go
git commit -m "feat(loadgen): add non-retrying room and member mutation RPCs"
```

---

### Task 5: Room read lane (`soak_roomread.go`)

**Files:**
- Create: `tools/loadgen/soak_roomread.go`
- Test: `tools/loadgen/soak_roomread_test.go`

**Interfaces:**
- Consumes: `soakRPCClient`, `soakReadSampleRecorder` (`soak_read.go:50`), Task 2's carriers.
- Produces:

```go
type soakRoomReader struct{ /* unexported */ }

func newSoakRoomReader(cfg soakRoomReadConfig, pool *soakRoomStatePool, rpc *soakRPCClient, recorder soakReadSampleRecorder, rng *rand.Rand, now func() time.Time) *soakRoomReader
func (r *soakRoomReader) ReadMixed(ctx context.Context) error
func (r *soakRoomReader) ListMembers(ctx context.Context, roomID string) (soakListMembersResponse, error)
func (r *soakRoomReader) RoomsInfo(ctx context.Context, roomIDs []string) error
func (r *soakRoomReader) SubscriptionList(ctx context.Context) error
func (r *soakRoomReader) RegisterCreatedRoom(roomID string)

type soakRoomReadConfig struct {
	SiteID         string
	BatchSize      int
	RequestTimeout time.Duration
}
```

`ReadMixed` picks one of the three read shapes with a fixed distribution (50% member list, 30% rooms-info batch, 20% subscription list), records a `soakReadSample`, and never creates a ledger operation. `RegisterCreatedRoom` lets the `room_create` lane add its rooms to this reader's target set — the only place created rooms receive traffic.

- [ ] **Step 1: Write the failing tests**

Create `tools/loadgen/soak_roomread_test.go` covering: each read shape hits the right subject; a transport error is recorded with the right `ErrorClass` and returns the error; `ReadMixed` records exactly one sample per call; `RegisterCreatedRoom` makes a new room eligible; an empty room set records a skipped sample rather than panicking. Reuse the `soakRoomOpsTransport` fake from Task 4 (it is in the same package). Name every test with a `Soak` prefix.

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL — `undefined: newSoakRoomReader`.

- [ ] **Step 3: Implement the reader**

Create `tools/loadgen/soak_roomread.go`. Subjects: `subject.MemberList(account, roomID, siteID)`, `subject.RoomsInfoBatchSubscribe(siteID)` with a `soakRoomsInfoRequest` body capped at `BatchSize` room IDs, and `subject.UserSubscriptionList(account, siteID)`. All three use `RetryMode: soakRetrySafe` — they are reads. Guard the created-room slice with the pool's mutex-free own `sync.Mutex` inside the reader.

- [ ] **Step 4: Run the tests and commit**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS.

```bash
git add tools/loadgen/soak_roomread.go tools/loadgen/soak_roomread_test.go
git commit -m "feat(loadgen): add the room read lane"
```

---

### Task 6: Mongo primary readbacks and ownership append (`soak_store.go`)

**Files:**
- Modify: `tools/loadgen/soak_store.go` (interfaces `:26-35`, `ReplaceOwnershipChunks` `:479`)
- Test: `tools/loadgen/soak_store_test.go`, `tools/loadgen/soak_seed_integration_test.go`

**Interfaces:**
- Produces:

```go
type soakRoomStateStore interface {
	RoomName(ctx context.Context, roomID string) (string, bool, error)
	IsRoomMember(ctx context.Context, roomID, account string) (bool, error)
	SubscriptionMuted(ctx context.Context, roomID, account string) (bool, bool, error)
	AppendOwnedRooms(ctx context.Context, runID string, roomIDs []string) error
}
```

All three read methods **must** run against the replica-set primary. `mongoutil.Connect` sets `readpref.SecondaryPreferred` (`pkg/mongoutil/mongo.go:120`), so a default-preference read could observe replication lag and turn "the secondary has not caught up" into a false `missing_after_deadline`. Use `mongoutil.CollectionWithReadPreference(col, readpref.Primary())` (`pkg/mongoutil/collection.go:26`) — the same pattern as `user-service/mongorepo/readpref.go`.

Every query must project precisely (CLAUDE.md): `rooms` → `{_id, name}`; `subscriptions` → `{_id, muted}`; `room_members` → `{_id}`. All three are covered by existing indexes (`room-service/store_mongo.go:105-124`): `rooms._id` is the primary key, `subscriptions (roomId, u.account)` is unique, `room_members (rid, member.type, member.id)` is unique.

`AppendOwnedRooms` writes a **new** ownership chunk document rather than replacing the set, preserving the teardown paging invariants (`soak_teardown.go`: cursor strictly increasing, chunk size ≤ `soakOwnershipChunkSize`).

- [ ] **Step 1: Write the failing integration tests**

Add to `tools/loadgen/soak_seed_integration_test.go` (build tag `integration`, `testutil.MongoDB`):

```go
func TestSoakStore_RoomStateReadbacksUsePrimary(t *testing.T) {
	db := testutil.MongoDB(t, "soakroomstate")
	store := &mongoSoakStore{db: db}
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, bson.M{"_id": "room-1", "name": "soak-channel"})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertOne(ctx, bson.M{
		"_id": "sub-1", "roomId": "room-1", "u": bson.M{"account": "user-2"}, "muted": true,
	})
	require.NoError(t, err)
	_, err = db.Collection("room_members").InsertOne(ctx, bson.M{
		"_id": "rm-1", "rid": "room-1", "member": bson.M{"type": "user", "id": "u2", "account": "user-2"},
	})
	require.NoError(t, err)

	name, found, err := store.RoomName(ctx, "room-1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "soak-channel", name)

	member, err := store.IsRoomMember(ctx, "room-1", "user-2")
	require.NoError(t, err)
	assert.True(t, member)

	muted, known, err := store.SubscriptionMuted(ctx, "room-1", "user-2")
	require.NoError(t, err)
	assert.True(t, known)
	assert.True(t, muted)

	_, found, err = store.RoomName(ctx, "missing-room")
	require.NoError(t, err)
	assert.False(t, found, "an absent room must be reported as not found, never as an error")
}

func TestSoakStore_AppendOwnedRoomsKeepsTeardownPaging(t *testing.T) {
	db := testutil.MongoDB(t, "soakownershipappend")
	store := &mongoSoakStore{db: db}
	ctx := context.Background()

	require.NoError(t, store.ReplaceOwnershipChunks(ctx, "run-1", [][]string{{"room-1", "room-2"}}))
	require.NoError(t, store.AppendOwnedRooms(ctx, "run-1", []string{"room-3"}))

	after := ""
	var collected []string
	for {
		page, err := store.NextOwnershipPage(ctx, "run-1", after, soakOwnershipChunkSize)
		require.NoError(t, err)
		if page == nil {
			break
		}
		require.Greater(t, page.Cursor, after, "the teardown cursor must strictly advance")
		collected = append(collected, page.RoomIDs...)
		after = page.Cursor
	}
	assert.ElementsMatch(t, []string{"room-1", "room-2", "room-3"}, collected)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `make test-integration SERVICE=tools/loadgen`
Expected: FAIL — `store.RoomName undefined`.

- [ ] **Step 3: Implement the store methods**

Read `soak_store.go:479-570` first to match the existing chunk-ID scheme, then implement. Sketch:

```go
func (s *mongoSoakStore) roomsPrimary() *mongo.Collection {
	return mongoutil.CollectionWithReadPreference(s.db.Collection("rooms"), readpref.Primary())
}

// RoomName reads the authoritative room name. The soak client connects with
// SecondaryPreferred, so a default-preference read here could report a stale
// name during replication lag and turn a healthy rename into a mismatch.
func (s *mongoSoakStore) RoomName(ctx context.Context, roomID string) (string, bool, error) {
	var document struct {
		Name string `bson:"name"`
	}
	err := s.roomsPrimary().FindOne(ctx, bson.D{{Key: "_id", Value: roomID}},
		options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 0}, {Key: "name", Value: 1}}),
	).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read soak room name for %q: %w", roomID, err)
	}
	return document.Name, true, nil
}
```

`IsRoomMember` queries `room_members` with `{rid, member.account}` projected to `{_id: 1}`. `SubscriptionMuted` queries `subscriptions` with `{roomId, u.account}` projected to `{muted: 1}` and returns `(muted, found, err)`. `AppendOwnedRooms` chunks the input at `soakOwnershipChunkSize` and inserts new chunk documents whose IDs sort after every existing chunk for the run.

- [ ] **Step 4: Run the tests and commit**

Run: `make test-integration SERVICE=tools/loadgen`
Expected: PASS.

```bash
git add tools/loadgen/soak_store.go tools/loadgen/soak_seed_integration_test.go
git commit -m "feat(loadgen): add primary-read room state lookups and appendable run ownership"
```

---

### Task 7: The `room_state` observer (`soak_roomverify.go`)

**Files:**
- Create: `tools/loadgen/soak_roomverify.go`
- Test: `tools/loadgen/soak_roomverify_test.go`

**Interfaces:**
- Consumes: `soakRoomReader` (Task 5), `soakRoomStateStore` (Task 6), `failureOperation` (`failure_ledger.go:170`).
- Produces:

```go
type soakRoomStateResult string

const (
	soakRoomStateMatched   soakRoomStateResult = "matched"
	soakRoomStateMismatch  soakRoomStateResult = "mismatch"
	soakRoomStateAbsent    soakRoomStateResult = "absent"
	soakRoomStateUnknown   soakRoomStateResult = "unknown"
)

type soakRoomStateVerifier struct{ /* unexported */ }

func newSoakRoomStateVerifier(reader *soakRoomReader, store soakRoomStateStore, metrics *Metrics, health *failureObserverHealth, now func() time.Time) *soakRoomStateVerifier
func (v *soakRoomStateVerifier) Verify(ctx context.Context, operation *failureOperation) (soakRoomStateResult, failureReason, error)
```

Attribute keys carried on the operation (set by Task 8):

```go
soakFailureAttributeTargetAccount   = "target_account"
soakFailureAttributeExpectedName    = "expected_name"
soakFailureAttributeExpectedMember  = "expected_member"  // "true" | "false"
soakFailureAttributeExpectedMuted   = "expected_muted"   // "true" | "false"
```

Verdict rules — these are the heart of the task:

| Situation | Result | Reason |
|---|---|---|
| room-service says the expected state holds | `matched` | — |
| room-service answers and the state contradicts the expectation | `mismatch` | type-specific |
| room-service answers "not there" **and** Mongo primary agrees | `absent` | type-specific |
| room-service answers "not there", Mongo primary says it *is* there | `matched` | — (RPC read lag; the authoritative source wins) |
| room-service unavailable, Mongo primary answers | Mongo's verdict | as above |
| both sources unavailable | `unknown` | — |

The caller (Task 8) converts `absent` into `missing_after_deadline` only past the deadline, and `unknown` always into `unverified`. The verifier never decides deadlines.

Every source attempt increments `SoakRoomStateSources{source, result}` with `source ∈ {room_service, mongo}` and `result ∈ {matched, mismatch, absent, unknown}`, and updates the observer health (`health.Set(up, at, reason)`).

- [ ] **Step 1: Write the failing tests**

Create `tools/loadgen/soak_roomverify_test.go` with a fake store:

```go
type soakRoomStateStoreStub struct {
	name        string
	nameFound   bool
	nameErr     error
	member      bool
	memberErr   error
	muted       bool
	mutedKnown  bool
	mutedErr    error
}
```

Table-driven tests must cover, at minimum:

1. `member_add` + room-service lists the account → `matched`.
2. `member_add` + room-service omits the account + Mongo says not a member → `absent`, reason `member_state_mismatch`... **no**: reason is `failureReasonRoomStateMissing` for an absent effect and `failureReasonMemberStateMismatch` only when a source reports the opposite state. Encode exactly that.
3. `member_remove` + room-service still lists the account + Mongo agrees → `absent` is wrong; the expected state is "not a member", and the account being present means the removal never landed → `absent` with reason `failureReasonRoomStateMissing`.
4. `member_add` + room-service RPC error + Mongo says member → `matched`.
5. `member_add` + both sources error → `unknown`.
6. `room_rename` + Mongo name equals `expected_name` → `matched`; different non-empty name → `mismatch` with `failureReasonRoomNameMismatch`; room absent → `absent` with `failureReasonRoomStateMissing`.
7. `mute_toggle` + `SubscriptionMuted` returns the expected value → `matched`; the opposite value → `mismatch` with `failureReasonMuteStateMismatch`; `known == false` → `absent` with `failureReasonRoomStateMissing`.
8. `room_create` + `RoomName` finds the room → `matched`; not found → `absent`.
9. Health flips to down on a source error and back to up on the next success.
10. `SoakRoomStateSources` counts one sample per source attempt.

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL — `undefined: newSoakRoomStateVerifier`.

- [ ] **Step 3: Implement the verifier**

Create `tools/loadgen/soak_roomverify.go` with a `switch operation.OperationType` dispatching to `verifyMember`, `verifyRename`, `verifyMute`, `verifyCreated`, each of which calls the RPC source then the Mongo source per the verdict table. Keep the "authoritative source wins" rule in one place:

```go
// resolve merges the two sources. Mongo primary is authoritative: a room-service
// answer of "not there" can be a read-path failure, while a primary read that
// finds the state proves the write landed. Only agreement — or an unreachable
// authoritative source — can produce an absence claim.
func resolve(rpc, authoritative soakRoomStateResult) soakRoomStateResult {
	switch {
	case authoritative == soakRoomStateMatched:
		return soakRoomStateMatched
	case authoritative == soakRoomStateUnknown && rpc == soakRoomStateMatched:
		return soakRoomStateMatched
	case authoritative == soakRoomStateUnknown:
		return soakRoomStateUnknown
	default:
		return authoritative
	}
}
```

- [ ] **Step 4: Run the tests and commit**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS.

```bash
git add tools/loadgen/soak_roomverify.go tools/loadgen/soak_roomverify_test.go
git commit -m "feat(loadgen): add the room state observer with a primary-read authoritative source"
```

---

### Task 8: Lane glue — intents, admission, reconciliation, create budget (`soak_roommember.go`)

**Files:**
- Create: `tools/loadgen/soak_roommember.go`
- Test: `tools/loadgen/soak_roommember_test.go`

**Interfaces:**
- Consumes: everything from Tasks 1, 3, 4, 6, 7; `failureLedger` (`failure_ledger.go:289`), `soakShareGate` (`soak_failure.go:620`).
- Produces:

```go
type soakRoomLaneConfig struct {
	RunID            string
	PersistGrace     time.Duration
	Deadline         time.Duration
	RetryInterval    time.Duration
	RoomCreateBudget int
	CreateRoomSize   int
}

type soakRoomLanes struct{ /* unexported */ }

func newSoakRoomLanes(cfg soakRoomLaneConfig, pool *soakRoomStatePool, mutator *soakRoomMutator, ledger *failureLedger, metrics *Metrics, recorder soakMutationSampleRecorder, now func() time.Time) *soakRoomLanes
func (l *soakRoomLanes) MemberMutation(ctx context.Context) error
func (l *soakRoomLanes) RoomMutation(ctx context.Context) error
func (l *soakRoomLanes) RoomCreate(ctx context.Context) error
func (l *soakRoomLanes) Reconcile(ctx context.Context, verifier *soakRoomStateVerifier) (bool, error)
func (l *soakRoomLanes) ProbeQuarantine(ctx context.Context, verifier *soakRoomStateVerifier) (bool, error)
```

Operation lifecycle for every mutation (identical shape for all three lanes):

1. Take an intent from the pool. If none is available, record `SoakRoomPoolExhausted{reason}` and return — this is a skipped dispatch, **not** an untracked operation.
2. Build a `failureOperation` with `SchemaVersion: 2`, a fresh `idgen.GenerateUUIDv7()` operation ID, `LifecycleState: failureOperationJournaled`, the lane's effects from Task 1, `Targets` (`roomId`, and `account` where applicable), and `Attributes` carrying the expected final state.
3. `ledger.Start(...)` — the intent is durable **before** the request goes out. On `errFailureLedgerCapacity` or a WAL error, do **not** send: unlike the message lane (which must keep offering load), a room mutation that is not journaled would leave untrackable state drift in Mongo. Record `FailureUntracked{reason="start"}`, release the pool lease, and return.
4. Issue the RPC.
5. Classify the reply:
   - marshal failure or `nats.ErrConnectionClosed` before the write → `ledger.Abandon(id, failureResultNotSent, now)` and release the lease with `failureResultNotSent`.
   - any other error → `ledger.Activate(id, now)` then `Observe(admission, unverified)`. **Never** `not_sent`, never a resend.
   - accepted → `Activate` then `Observe(admission, good)`.
   - explicit rejection (`forbidden`, `conflict`, `bad_request`, `not_found`) → `Activate` then `ObserveWithReason(admission, bad, failureReasonAdmissionRejected)`.
6. The pool lease is released when the ledger finalizes the operation, which happens inside `Reconcile`. Keep an in-memory `map[operationID]soakMemberIntent` (bounded by the ledger capacity) so `Reconcile` can settle the pool with the terminal result.

`Reconcile` mirrors `soakFailureReconciler.Try` (`soak_failure.go:503`) but claims only operations whose lane is one of the three room lanes, calls the Task 7 verifier, and maps:

```go
	switch result {
	case soakRoomStateMatched:
		observation, reason = failureObservationGood, failureReasonNone
	case soakRoomStateMismatch:
		observation = failureObservationBad
	case soakRoomStateAbsent:
		if now.Before(operation.Deadline) {
			return true, l.ledger.ReleaseClaim(operation.ID, now.Add(l.cfg.RetryInterval))
		}
		observation = failureObservationMissingAfterDeadline
	default:
		if now.Before(operation.Deadline) {
			return true, l.ledger.ReleaseClaim(operation.ID, now.Add(l.cfg.RetryInterval))
		}
		observation, reason = failureObservationUnverified, failureReasonNone
	}
```

Note that `failureOperationResult` (`failure_ledger.go:1310`) already downgrades `missing_after_deadline` to `unverified` unless admission observed `good` — do not re-implement that rule here.

`RoomCreate` additionally: decrements the budget under a mutex, sets `SoakRoomCreateBudgetRemaining`, returns early once the budget is spent (the other lanes keep running), and on a terminal `good` calls `store.AppendOwnedRooms` and `reader.RegisterCreatedRoom`.

- [ ] **Step 1: Write the failing tests**

Create `tools/loadgen/soak_roommember_test.go`. Required cases:

1. `TestSoakRoomLanes_MemberMutationJournalsIntentBeforeSending` — a mutator stub records the call order against a journal spy; assert the WAL append precedes the request.
2. `TestSoakRoomLanes_AmbiguousMutationIsUnverifiedNotNotSent` — transport returns `nats.ErrTimeout`; assert the operation's admission observation is `unverified`, the operation is still active, and no second request was issued.
3. `TestSoakRoomLanes_LocalFailureIsNotSent` — the mutator returns a marshal error before any request; assert `ledger.Snapshot().Results[failureResultNotSent] == 1`.
4. `TestSoakRoomLanes_RejectionIsBadWithBoundedReason` — a `forbidden` envelope yields `bad` with `failureReasonAdmissionRejected`.
5. `TestSoakRoomLanes_ReconcileMatchedFinalizesGood`.
6. `TestSoakRoomLanes_ReconcileAbsentBeforeDeadlineRetries`.
7. `TestSoakRoomLanes_ReconcileAbsentAfterDeadlineWithAcceptedAdmissionIsMissing`.
8. `TestSoakRoomLanes_ReconcileAbsentAfterDeadlineWithoutAcceptedAdmissionIsUnverified` — the rule that protects against calling an ambiguous request data loss.
9. `TestSoakRoomLanes_LedgerRejectionSkipsTheRequest` — a journal stub that fails `Append`; assert no RPC was issued and `FailureUntracked{start}` incremented.
10. `TestSoakRoomLanes_RoomCreateStopsAtBudgetWithoutStoppingOtherLanes`.
11. `TestSoakRoomLanes_RoomCreateRegistersOwnershipOnSuccess`.
12. `TestSoakRoomLanes_QuarantineProbeResolvesCandidate`.

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL — `undefined: newSoakRoomLanes`.

- [ ] **Step 3: Implement the lanes**

Create `tools/loadgen/soak_roommember.go` per the lifecycle above.

- [ ] **Step 4: Run the tests and commit**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS.

```bash
git add tools/loadgen/soak_roommember.go tools/loadgen/soak_roommember_test.go
git commit -m "feat(loadgen): journal room and member mutations through the evidence ledger"
```

---

### Task 9: Config, workload lanes, and runtime wiring

**Files:**
- Modify: `tools/loadgen/soak_config.go`
- Modify: `tools/loadgen/soak_workload.go:13-20, 22-36, 362-371`
- Modify: `tools/loadgen/soak_main.go:747-844, 991-1002`
- Test: `tools/loadgen/soak_config_test.go`, `tools/loadgen/soak_workload_test.go`

**Interfaces:**
- Produces: `soakWorkloadActions{..., MemberMutation, RoomMutation, RoomRead, RoomCreate soakWorkloadAction}` and the matching rates on `soakWorkloadConfig`.

New config fields on `soakConfig`:

```go
	MemberMutationRate       float64       `env:"MEMBER_MUTATION_RATE"        envDefault:"2"`
	RoomMutationRate         float64       `env:"ROOM_MUTATION_RATE"          envDefault:"1"`
	RoomReadRate             float64       `env:"ROOM_READ_RATE"              envDefault:"20"`
	RoomCreateRate           float64       `env:"ROOM_CREATE_RATE"            envDefault:"0.05"`
	RoomCreateBudget         int           `env:"ROOM_CREATE_BUDGET"          envDefault:"2000"`
	RoomCreateSize           int           `env:"ROOM_CREATE_SIZE"            envDefault:"5"`
	RoomReconcileReadShare   float64       `env:"ROOM_RECONCILE_READ_SHARE"   envDefault:"0.5"`
	MemberQuarantineMax      int           `env:"MEMBER_QUARANTINE_MAX"       envDefault:"10000"`
```

Validation to add in `validateSoakConfig`:

- the four rates are non-negative and finite (reuse `validateNonNegativeRate`);
- `RoomReconcileReadShare` is in `(0, 1]`;
- `RoomCreateBudget >= 0`, `RoomCreateSize` in `[2, 50]`, `MemberQuarantineMax` in `[1, 1000000]`;
- **the reconciliation capacity check**, which is the one that prevents a silently unverifiable run:

```go
	// Room and member reconciliation borrows room_read slots, so the read lane
	// must be able to retire mutations at least as fast as they are created.
	// Without this the unresolved backlog grows without bound and every mutation
	// eventually expires unverified — a run that cannot conclude anything.
	mutationRate := cfg.MemberMutationRate + cfg.RoomMutationRate + cfg.RoomCreateRate
	if mutationRate > 0 {
		reconcileCapacity := cfg.RoomReadRate * cfg.RoomReconcileReadShare
		if reconcileCapacity < mutationRate {
			return fmt.Errorf(
				"SOAK_ROOM_READ_RATE %.3f at SOAK_ROOM_RECONCILE_READ_SHARE %.3f reconciles %.3f ops/s, "+
					"below the %.3f ops/s produced by the room and member mutation lanes; "+
					"raise SOAK_ROOM_READ_RATE or lower the mutation rates",
				cfg.RoomReadRate, cfg.RoomReconcileReadShare, reconcileCapacity, mutationRate)
		}
	}
```

- [ ] **Step 1: Write the failing tests**

Add to `tools/loadgen/soak_config_test.go`:

```go
func TestValidateSoakConfig_RejectsUnderprovisionedRoomReconciliation(t *testing.T) {
	cfg := validSoakConfigForTest()
	cfg.MemberMutationRate = 5
	cfg.RoomMutationRate = 5
	cfg.RoomReadRate = 4
	cfg.RoomReconcileReadShare = 0.5

	err := validateSoakConfig(&cfg, "chat")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "SOAK_ROOM_READ_RATE")
}

func TestValidateSoakConfig_AcceptsRoomLaneDefaults(t *testing.T) {
	cfg := validSoakConfigForTest()

	require.NoError(t, validateSoakConfig(&cfg, "chat"))
}

func TestValidateSoakConfig_RejectsUnsafeLedgerEpoch(t *testing.T) {
	cfg := validSoakConfigForTest()
	cfg.LedgerEpoch = "../escape"

	require.Error(t, validateSoakConfig(&cfg, "chat"))
}
```

`validSoakConfigForTest` already exists in that file — extend it with the new defaults (mutation 2 + 1 + 0.05 = 3.05 ops/s against a 20 × 0.5 = 10 ops/s reconcile capacity, comfortably inside the gate).

Add to `tools/loadgen/soak_workload_test.go`:

```go
func TestSoakWorkload_DispatchesRoomAndMemberLanes(t *testing.T) {
	dispatched := make(map[string]int)
	var mu sync.Mutex
	workload := newSoakWorkload(&soakWorkloadConfig{
		RunID: "run-1", Continuous: true, HeartbeatInterval: time.Hour,
		SendRate: 1, ReadRate: 1, MemberMutationRate: 1, RoomMutationRate: 1,
		RoomReadRate: 1, RoomCreateRate: 1, MaxInFlight: 4,
	}, stubLifecycleStore(t), soakWorkloadActions{
		Send: noopSoakAction, Read: noopSoakAction,
		MemberMutation: countingSoakAction(&mu, dispatched, "member_mutation"),
		RoomMutation:   countingSoakAction(&mu, dispatched, "room_mutation"),
		RoomRead:       countingSoakAction(&mu, dispatched, "room_read"),
		RoomCreate:     countingSoakAction(&mu, dispatched, "room_create"),
	}, recordingSoakDispatcher(&mu, dispatched), fixedNow, nil)

	_, _ = workload.Run(canceledAfter(t, 50*time.Millisecond))

	mu.Lock()
	defer mu.Unlock()
	for _, lane := range []string{"member_mutation", "room_mutation", "room_read", "room_create"} {
		assert.Positive(t, dispatched[lane], "lane %s must be dispatched", lane)
	}
}
```

Follow the existing helpers in `soak_workload_test.go`; if the named helpers above do not exist, write them in that file with these exact names.

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL — unknown fields `MemberMutationRate`, `RoomRead`.

- [ ] **Step 3: Extend the workload**

In `soak_workload.go`, add the four actions to `soakWorkloadActions`, the four rates to `soakWorkloadConfig`, and four entries to `lanes()`:

```go
		{name: "member_mutation", rate: w.cfg.MemberMutationRate, action: w.actions.MemberMutation},
		{name: "room_mutation", rate: w.cfg.RoomMutationRate, action: w.actions.RoomMutation},
		{name: "room_read", rate: w.cfg.RoomReadRate, action: w.actions.RoomRead},
		{name: "room_create", rate: w.cfg.RoomCreateRate, action: w.actions.RoomCreate},
```

No other change is needed: `Run` already configures, paces, and meters every lane returned by `lanes()`, so `loadgen_soak_configured_rate`, `intended`, `dispatched`, `scheduler_underrun`, `lane_saturation`, and `global_saturation` all pick up the new lanes for free.

- [ ] **Step 4: Wire the runtime**

In `soak_main.go` `runSoakWorkload`, after the existing verifier construction (`:735-744`), add:

```go
	roomPool, err := newSoakRoomStatePool(
		&topology, cfg.Soak.MemberQuarantineMax, metrics, rand.New(rand.NewSource(seed+8)),
	)
	if err != nil {
		slog.Error("prepare soak room state pool", "error", err)
		return 1
	}
	roomMutator := newSoakRoomMutator(cfg.SiteID, rpc, soakRequestTimeout, now)
	roomReader := newSoakRoomReader(
		soakRoomReadConfig{SiteID: cfg.SiteID, BatchSize: 20, RequestTimeout: soakRequestTimeout},
		roomPool, rpc, recorders.read, rand.New(rand.NewSource(seed+9)), now,
	)
	roomStateHealth := newFailureObserverHealth(failureObserverRoomState, now())
	roomVerifier := newSoakRoomStateVerifier(roomReader, store, metrics, roomStateHealth, now)
	roomLanes := newSoakRoomLanes(soakRoomLaneConfig{
		RunID: cfg.Soak.RunID, PersistGrace: cfg.Soak.PersistGrace,
		Deadline: cfg.Soak.ReconcileDeadline, RetryInterval: cfg.Soak.ReconcileRetryInterval,
		RoomCreateBudget: cfg.Soak.RoomCreateBudget, CreateRoomSize: cfg.Soak.RoomCreateSize,
	}, roomPool, roomMutator, ledger, metrics, recorders.mutation, now)
	roomReconcileGate := newSoakShareGate(cfg.Soak.RoomReconcileReadShare)
```

and the four actions:

```go
		MemberMutation: func(actionCtx context.Context, _ bool) error {
			if err := roomLanes.MemberMutation(actionCtx); err != nil {
				slog.Error("run soak member mutation", "error", err)
			}
			return nil
		},
		RoomMutation: func(actionCtx context.Context, _ bool) error {
			if err := roomLanes.RoomMutation(actionCtx); err != nil {
				slog.Error("run soak room mutation", "error", err)
			}
			return nil
		},
		RoomRead: func(actionCtx context.Context, _ bool) error {
			// Room and member reconciliation borrows read slots so verification
			// adds no unbudgeted RPS, capped by its share so a fault-time backlog
			// cannot starve the production-like room read mix.
			if roomReconcileGate.Allow() {
				if reconciled, err := roomLanes.Reconcile(actionCtx, roomVerifier); err != nil {
					slog.Error("reconcile soak room operation", "error", err)
				} else if reconciled {
					return nil
				}
				if probed, err := roomLanes.ProbeQuarantine(actionCtx, roomVerifier); err != nil {
					slog.Error("probe quarantined soak member candidate", "error", err)
				} else if probed {
					return nil
				}
			}
			if err := roomReader.ReadMixed(actionCtx); err != nil {
				slog.Error("run soak room read", "error", err)
			}
			return nil
		},
		RoomCreate: func(actionCtx context.Context, _ bool) error {
			if err := roomLanes.RoomCreate(actionCtx); err != nil {
				slog.Error("run soak room create", "error", err)
			}
			return nil
		},
```

Pass the four rates into `newSoakWorkload`'s config literal, and extend `soakTargetRates` (`:991`) with the new actions so the printed report covers them.

- [ ] **Step 5: Run everything**

Run: `make test SERVICE=tools/loadgen && make lint`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add tools/loadgen/soak_config.go tools/loadgen/soak_config_test.go \
        tools/loadgen/soak_workload.go tools/loadgen/soak_workload_test.go \
        tools/loadgen/soak_main.go
git commit -m "feat(loadgen): run the room and member lanes inside the continuous soak"
```

---

### Task 10: Integration test against real NATS and Mongo

**Files:**
- Create/modify: `tools/loadgen/soak_roommember_integration_test.go` (build tag `integration`)

The test must drive the real ledger, a real WAL on a temp dir, and stubbed room-service responses over a real NATS connection from `testutil.NATS(t)`, plus a real Mongo from `testutil.MongoDB(t, ...)`.

- [ ] **Step 1: Write the test**

```go
//go:build integration

package main

func TestSoakRoomMemberEvidence_SurvivesRestartAndReconciles(t *testing.T) {
	// 1. Start a fake room-service responder on subject.MemberAddPattern(siteID)
	//    that replies {"status":"accepted"} and records the request.
	// 2. Run one MemberMutation through soakRoomLanes with a real WAL directory.
	// 3. Close the ledger without reconciling.
	// 4. Reopen the ledger from the same WAL path and assert the operation was
	//    recovered (Snapshot().Recovered == 1) and is still active.
	// 5. Insert the corresponding room_members document in Mongo.
	// 6. Reconcile and assert the terminal result is failureResultGood.
}
```

Implement it fully following `tools/loadgen/failure_evidence_integration_test.go` as the model for WAL reopen and `tools/loadgen/soak_seed_integration_test.go` for Mongo setup. Add a second case that never inserts the Mongo document, advances the clock past the deadline, and asserts `failureResultMissingAfterDeadline` — and a third that additionally makes admission `unverified` and asserts the terminal result is `failureResultUnverified` rather than a data-loss claim.

- [ ] **Step 2: Run it**

Run: `make test-integration SERVICE=tools/loadgen`
Expected: PASS.

- [ ] **Step 3: Run both coverage gates**

Run: `make coverage-loadgen-soak && make coverage-loadgen-failure`
Expected: PASS. If a new `soak_*.go` file is below 90%, add the missing table cases — do not lower the threshold.

- [ ] **Step 4: Commit**

```bash
git add tools/loadgen/soak_roommember_integration_test.go
git commit -m "test(loadgen): cover room and member evidence recovery end to end"
```

---

### Task 11: Helm and Compose wiring

**Files:**
- Modify: `tools/loadgen/deploy/k8s/values.yaml`, `values.schema.json`, `templates/configmap.yaml`, `templates/_helpers.tpl`
- Modify: `tools/loadgen/deploy/docker-compose.yml`
- Modify: `tools/loadgen/deploy/k8s/README.md`

- [ ] **Step 1: Add the values**

In `values.yaml`, under `ledger:` add `epoch: v1`, and under `soak:`:

```yaml
  memberMutationRate: "2"
  roomMutationRate: "1"
  roomReadRate: "20"
  roomCreateRate: "0.05"
  roomCreateBudget: "2000"
  roomCreateSize: "5"
  memberQuarantineMax: "10000"
```

and under `ledger:` add `roomReconcileReadShare: 0.5`.

- [ ] **Step 2: Add the ConfigMap keys**

In `templates/configmap.yaml`:

```yaml
  SOAK_LEDGER_EPOCH: {{ .Values.ledger.epoch | quote }}
  SOAK_ROOM_RECONCILE_READ_SHARE: {{ .Values.ledger.roomReconcileReadShare | quote }}
  SOAK_MEMBER_MUTATION_RATE: {{ .Values.soak.memberMutationRate | quote }}
  SOAK_ROOM_MUTATION_RATE: {{ .Values.soak.roomMutationRate | quote }}
  SOAK_ROOM_READ_RATE: {{ .Values.soak.roomReadRate | quote }}
  SOAK_ROOM_CREATE_RATE: {{ .Values.soak.roomCreateRate | quote }}
  SOAK_ROOM_CREATE_BUDGET: {{ .Values.soak.roomCreateBudget | quote }}
  SOAK_ROOM_CREATE_SIZE: {{ .Values.soak.roomCreateSize | quote }}
  SOAK_MEMBER_QUARANTINE_MAX: {{ .Values.soak.memberQuarantineMax | quote }}
```

- [ ] **Step 3: Add chart validation**

In `_helpers.tpl`'s `cassandra-soak.validate`, add the same reconciliation-capacity guard the binary enforces, so a bad values file fails at `helm template` instead of at pod start:

```yaml
{{- $mutation := addf (float64 .Values.soak.memberMutationRate) (addf (float64 .Values.soak.roomMutationRate) (float64 .Values.soak.roomCreateRate)) -}}
{{- $capacity := mulf (float64 .Values.soak.roomReadRate) (float64 .Values.ledger.roomReconcileReadShare) -}}
{{- if and (gt $mutation 0.0) (lt $capacity $mutation) -}}
{{- fail "soak.roomReadRate * ledger.roomReconcileReadShare must be at least the sum of the room and member mutation rates" -}}
{{- end -}}
{{- if not .Values.ledger.epoch -}}
{{- fail "ledger.epoch is required; bump it whenever the loadgen image changes the ledger contract" -}}
{{- end -}}
```

- [ ] **Step 4: Update the JSON schema**

Add the new keys to `values.schema.json` with the same types as their neighbours (all the soak rates are strings in this chart; `roomReconcileReadShare` is a number like `reconcileReadShare`).

- [ ] **Step 5: Validate**

Run: `make validate-loadgen-k8s`
Expected: PASS.

Also verify the guard fires:

Run: `helm template cassandra-soak tools/loadgen/deploy/k8s -f tools/loadgen/deploy/k8s/values.yaml --set phase=soak --set soak.roomReadRate=1`
Expected: FAIL with the reconciliation-capacity message.

- [ ] **Step 6: Mirror the knobs into Compose and commit**

```bash
git add tools/loadgen/deploy
git commit -m "feat(loadgen): expose room and member lane rates and the ledger epoch in the chart"
```

---

### Task 12: Documentation and final verification

**Files:**
- Modify: `docs/load-testing/loadgen-failure-observation.md`
- Modify: `tools/loadgen/README.md`
- Modify: `tools/loadgen/deploy/grafana/dashboards/loadtest.json`

- [ ] **Step 1: Document the lanes and rules**

Append to `docs/load-testing/loadgen-failure-observation.md`:

- a "Room and Member Lanes" section listing the four lanes, their operation types, and the per-lane observer sets;
- the `room_state` observer's two sources and the authoritative-source rule, stating explicitly that a MongoDB read uses the primary because the shared client is `secondaryPreferred`;
- the rule that `missing_after_deadline` requires an accepted admission;
- the candidate ring, quarantine, and the statement that quarantine overflow is a reversible traffic degradation, never a ledger invalidation, with the gate operators should use instead (`loadgen_soak_lane_attempts_total{outcome="sent"} < 90% of configured` over the window ⇒ that window is INCONCLUSIVE for that lane only — `dispatched` counts scheduler slots and stays flat when a lane finds no target, so it cannot express this);
- the `room_create` budget and the fact that created rooms are registered into run ownership so teardown removes them, and receive read traffic only;
- the `SOAK_LEDGER_EPOCH` contract: `runId` owns the topology, `epoch` owns the journal; bump the epoch on any image upgrade that changes the ledger contract; older journals are retained, never replayed, and surfaced by `loadgen_failure_abandoned_journals`; treat one reconcile deadline either side of an epoch switch as INCONCLUSIVE;
- the new configuration table rows and the new metric table rows.

**Do not** touch `docs/client-api.md` or its derived views — this change adds no client-facing handler and no `pkg/model` request/reply field.

- [ ] **Step 2: Add dashboard panels**

Add panels for `loadgen_soak_room_candidates`, `loadgen_soak_room_create_budget_remaining`, `loadgen_soak_room_state_source_total`, and per-lane `loadgen_failure_operations_total{lane=~"member_mutation|room_mutation|room_create"}`. Keep the existing panel IDs stable and append new ones.

- [ ] **Step 3: Full verification**

Run each and confirm output before claiming completion:

```bash
make fmt
make lint
make test SERVICE=tools/loadgen
make test-integration SERVICE=tools/loadgen
make coverage-loadgen-soak
make coverage-loadgen-failure
make validate-loadgen-k8s
make sast
```

- [ ] **Step 4: Commit and push**

```bash
git add docs/load-testing/loadgen-failure-observation.md tools/loadgen/README.md \
        tools/loadgen/deploy/grafana/dashboards/loadtest.json
git commit -m "docs(loadgen): document the room and member soak lanes and the ledger epoch"
git push -u origin claude/room-member-soak-expansion-6l6qq3
```

---

### Task 13: Presence and read-receipt lanes (added mid-implementation)

The first round excludes push notification, federation, and mass-WSS client
scenarios only. Presence and read receipts fell into neither exclusion and were
otherwise uncovered, so both were added to this same round.

**Files:**
- Add: `tools/loadgen/soak_presence.go`, `tools/loadgen/soak_presence_test.go`
- Modify: `soak_roomstate.go`, `soak_roomops.go`, `soak_roomverify.go`, `soak_roommember.go`, `failure_ledger.go`, `soak_rpc.go`, `soak_wire.go`, `soak_store.go`, `soak_config.go`, `soak_workload.go`, `soak_main.go`, `metrics.go`
- Modify: Helm values/schema/configmap/helpers, Compose, dashboard, both docs

- [x] **Step 1: `read_receipt` lane, ledger-tracked**

`messageRead` is a synchronous room-service write to `subscriptions.lastSeenAt`,
so it reuses the whole room-lane machinery: `soakRPCMessageRead` with
`soakRetryNever`, an intent journaled before send, and reconciliation through
the existing `room_state` observer via a new `subscription_read` effect.

The cursor is monotonic, which makes it verifiable without loadgen's clock: the
lane journals the previously confirmed cursor as `read_baseline_unix_ms` and
`classifySoakReadCursor` compares it against the value read back. Both
timestamps are server-written. No baseline yet ⇒ any present cursor is `good`;
a cursor that moved backwards is `mismatch`, not loss.

The lane borrows room-read reconciliation slots, so `SOAK_READ_RECEIPT_RATE` is
counted in `validateSoakRoomLaneConfig` and in the chart's equivalent guard.

- [x] **Step 2: `presence` lane, deliberately outside the ledger**

Presence signals are core NATS fire-and-forget publishes: buffered client-side
during an outage and flushed on reconnect. A successful publish is not evidence
of delivery and a failed one is not evidence of loss, so journaling them would
manufacture verdicts. `soakPresenceLane` therefore keeps no ledger operations.

Evidence comes from `queryBatch` alone. The lane holds its own view of the
connections it announced and re-queries the same set, suppressing comparison in
two windows where disagreement is legal: within `SOAK_PRESENCE_SETTLE` of a
publish, and past `SOAK_PRESENCE_TTL` where presence-service may legitimately
have expired the connection. `SOAK_PRESENCE_QUERY_SHARE` splits lane slots
between signalling and verifying.

- [x] **Step 3: Verification**

Same gate list as Task 12, plus new unit tests for the presence lane, the read
cursor pool state, the cursor classifier, and integration tests for the
read-receipt lane and `SubscriptionLastSeen`.

---

## Self-Review

**Spec coverage**

| Requirement from the task | Task |
|---|---|
| `member_mutation` with paired add/remove | 3, 4, 8 |
| Reusable candidates, no pool exhaustion | 3 |
| No conflicting operations on one room/account | 3 (leases) |
| `room_mutation`: rename + mute/unmute over a fixed room pool | 3, 4, 8 |
| `room_read`: room list, member/subscription list, state readback | 5, 7 |
| `room_create`: low rate, total budget, stops without stopping other lanes | 8, 9 |
| Durable intent before send | 8 (step 3 of the lifecycle) |
| Expected final state + deadline recorded | 8 (attributes), 1 (effects) |
| Authoritative room/member API or Mongo readback reconciliation | 7 |
| Terminal outcome for add/remove, mute/unmute, rename, create | 1 (effects), 8 (reconcile) |
| No blind resend, no `not_sent` on ambiguity | 4 (`soakRetryNever`), 8 (classification) |
| `not_sent` only for proven local failures | 8 |
| Reconciliation resumes from PVC/WAL after pod replacement | 10 (integration test) |
| Read-only lanes need no correctness ledger | 5 |
| Per-lane configured/intended/dispatched | 9 (free from `lanes()`) |
| Scheduler underrun, lane/global saturation | 9 (free from `lanes()`) |
| Request latency/error/result | 4, 5 (`soakReadSample`/`soakMutationSample` recorders) |
| Terminal outcome and bounded reason | 1 (reasons), 8 |
| No high-cardinality labels | 2 (test asserts it) |
| Helm room/member lane rates, no new deployment or phase | 11 |
| New `runId` per upgrade / incompatible contract handling | 1 (epoch), 12 (docs) |
| `read_receipt`: monotonic cursor verified without loadgen's clock | 13 |
| `presence`: signal + batch-query lane, no ledger by design | 13 |

**Placeholder scan:** every code step carries real code; every test step carries real test bodies or an explicit enumerated case list with exact names; every run step names the exact `make` target and the expected result.

**Type consistency:** `soakRoomStatePool`, `soakRoomMutator`, `soakRoomReader`, `soakRoomStateVerifier`, and `soakRoomLanes` are declared once in their producing task and referenced with identical names and signatures in Tasks 8–10. `failureObserverRoomState`, the four effects, the four reasons, and the three lane constants are declared in Task 1 and used unchanged thereafter.
