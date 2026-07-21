# Design: make `cassandra.Message` safe to JSON-decode with reactions

**Date:** 2026-07-20
**Status:** Approved (design phase)
**Scope:** General — fix the `cassandra.Reactions` type contract so any current or future
JSON consumer can decode a `cassandra.Message` that carries reactions.

## 1. Problem

Decoding a `cassandra.Message` that carries reactions fails at runtime:

```
json: cannot unmarshal object into Go struct field ...reactions of type cassandra.Reactions
```

### Root cause

`cassandra.Reactions` is `map[ReactionKey]ReactorInfo` (`pkg/model/cassandra/reactions.go:31`)
— a map keyed by a **struct** (`ReactionKey`). It defines a custom `MarshalJSON`
(`reactions.go:50`) that regroups the map into the client wire shape
`map<emoji, [{account, displayName}]>`, but it defines **no `UnmarshalJSON`**.

Go's JSON decoders (stdlib `encoding/json` and `sonic`) can only decode a JSON object
into a map whose key is a `string`, an integer, or a `TextUnmarshaler` — never a struct.
So decoding any JSON that contains a non-empty `reactions` object into a `cassandra.Message`
errors out. It only fails when reactions are present: the field is `omitempty`, so a nil
map is dropped from the wire and decode never reaches the struct-key path.

### Blast radius (from a codebase scan)

- **The single unguarded production decode is `user-service/historyclient/client.go:45`.**
  It decodes each site's thread-list reply (`model.ThreadSubscriptionListResponse` →
  `[]ThreadListItem` → `*cassandra.Message` for `ParentMessage`/`LastMessage`) and passes
  the messages straight through to the client. When a hydrated parent/last message carries
  a reaction, decode fails and the whole page errors.
- **`model.ThreadListItem` (`pkg/model/threadlist.go:26-27`) is the only cross-service wire
  type that embeds a full `*cassandra.Message`.**
- **Four other cross-service consumers already avoid the problem** by decoding into narrow
  projection structs that omit `Reactions` — `room-service/reader_history.go`,
  `notification-worker/parent_fetcher.go`, `broadcast-worker/parent_fetcher.go`,
  `message-gatekeeper/fetcher_history.go`. Projection is the codebase's established
  convention for consuming a `cassandra.Message` over the wire; thread-list is the lone
  exception because it forwards the whole message (including reactions) to the client.
- **history-service's own message responses are marshal-only** — they live in `internal/`
  and are never decoded by another Go module (producers only).
- **`cassandra.QuotedParentMessage` is unaffected** — it has no struct-keyed map.

### Key constraint: the client wire shape is already lossy

`docs/client-api.md` fixes the reactions client contract as
`reactions: map<emoji, [{account, displayName}]>`, sorted per emoji by `reactedAt` ascending
(FIFO — oldest first), same-millisecond ties broken by `account` ASC. Only `account` and
`displayName` ever reach the frontend. The other `ReactorInfo` fields (`userId`, `chnName`,
`account`, real `reactedAt`) are Cassandra-internal — populated via gocql when reading a row,
never serialized to a client. This is exactly what `MarshalJSON` emits today.

Because the encode is intentionally lossy, a **lossless decode is impossible without a
lossless encode**, which would conflict with the client contract. Therefore a decode can
only ever recover `{emoji, account, displayName}` plus order — and that is all any consumer
observes.

## 2. Chosen approach

**Symmetric codec on the type.** Give `cassandra.Reactions` an `UnmarshalJSON` that inverts
`MarshalJSON`, so the type is safe to decode anywhere. Rejected alternatives:

- **Projection DTO for thread-list** (match the other four sites): smallest change, but a
  projection that omits `Reactions` would drop reactions from the thread-list messages the
  client sees, and it leaves `cassandra.Message` unsafe to decode elsewhere — contrary to the
  general scope.
- **Presenter split** (lossless transport codec + separate client presenter): architecturally
  purest, but the grouped client shape is emitted at many marshal sites (all of
  history-service's message responses plus the live-delivery workers), so it is a large, risky
  refactor for fidelity no consumer needs. YAGNI.

## 3. Detailed change surface

### 3.1 `pkg/model/cassandra/reactions.go`

**Add a transient display-name carrier to `ReactorInfo`:**

```go
type ReactorInfo struct {
    UserID    string    `json:"userId"    cql:"user_id"`
    EngName   string    `json:"engName"   cql:"eng_name"`
    ChnName   string    `json:"chnName"   cql:"chn_name"`
    Account   string    `json:"account"   cql:"account"`
    ReactedAt time.Time `json:"reactedAt" cql:"reacted_at"`
    // DisplayName is a transient carrier populated only when a Reactions value was
    // decoded from the client wire form (see UnmarshalJSON). MarshalJSON prefers it
    // over composing from EngName/ChnName. json:"-" keeps it off the wire; cql:"-"
    // keeps gocql from binding/scanning it into the Cassandra UDT.
    DisplayName string `json:"-" cql:"-"`
}
```

**`MarshalJSON` — prefer the carried display name, else compose (unchanged behaviour when
`DisplayName` is empty):**

```go
dn := v.DisplayName
if dn == "" {
    dn = displayfmt.CombineWithFallback(v.EngName, v.ChnName, k.UserAccount)
}
// ...use dn as reactionUser.DisplayName
```

For any message read from Cassandra (the normal client path) `DisplayName` is empty, so the
composed value is used and the output is **byte-identical** to today.

**Add `UnmarshalJSON` (inverse of `MarshalJSON`):**

```go
func (r *Reactions) UnmarshalJSON(data []byte) error {
    if string(data) == "null" {
        *r = nil
        return nil
    }
    var grouped map[string][]reactionUser
    if err := json.Unmarshal(data, &grouped); err != nil {
        return fmt.Errorf("decode reactions: %w", err)
    }
    out := make(Reactions, len(grouped))
    for emoji, users := range grouped {
        for i, u := range users {
            out[ReactionKey{Emoji: emoji, UserAccount: u.Account}] = ReactorInfo{
                Account:     u.Account,
                DisplayName: u.DisplayName,
                // Index-based synthetic time so MarshalJSON's (reactedAt, account) sort
                // replays the original per-emoji array order; distinct per position.
                ReactedAt: time.UnixMilli(int64(i)),
            }
        }
    }
    *r = out
    return nil
}
```

Behaviour:
- `null` → nil map (re-marshals to `null`).
- `{}` → non-nil empty map (re-marshals to `{}`). In practice `omitempty` on the struct field
  means `{}` is not produced via a `Message`, but the type-level codec handles it for symmetry.
- Non-empty object → one map entry per `(emoji, account)`; FIFO order preserved via the
  synthetic index.
- Malformed value (e.g. a non-array under an emoji) → wrapped error.

### 3.2 Tests (TDD, type + model layer)

Follow Red → Green → Refactor.

`pkg/model/cassandra/reactions_test.go` — new `TestReactions_UnmarshalJSON`:
- `null` → nil map.
- `{}` → non-nil empty map.
- single reactor: correct key, `Account`, and carried `DisplayName`.
- multiple emoji / multiple reactors: correct distinct keys.
- **wire stability**: `Marshal → Unmarshal → Marshal` is byte-identical (asserts FIFO order
  preservation).
- invalid JSON → error.

`pkg/model/cassandra/reactions_test.go` — new `TestMessage_DecodeWithReactions`: a
`cassandra.Message` with reactions marshals and decodes cleanly (the regression test for the
reported failure).

`pkg/model/threadlist_test.go` — extend `TestThreadListItemJSON_WithMessages` so the parent
message carries a reaction (closes the blind spot that let the bug ship) and assert it
survives the round trip.

Coverage: keep `pkg/model/cassandra` at/above the repo's 80% floor; the new codec paths
(including error and null/empty branches) are all exercised.

### 3.3 Docs

- **`CLAUDE.md`**: update the note that currently says
  `message-gatekeeper/fetcher_history.go` projects a subset "because that type embeds the
  marshal-only struct-keyed `Reactions` map whose decoder sonic rejects." Reword to reflect
  that `Reactions` now has a symmetric (lossy in never-transmitted fields) codec, so a full
  `cassandra.Message` decodes; the projection is retained for its other reasons (field
  minimization / id authority), not because decode is impossible.
- **`docs/client-api.md`**: **no change** — the client wire shape and ordering are identical.

## 4. Non-goals

- No change to the client-facing wire shape or ordering.
- The four existing projection workarounds and their comments stay as-is.
- No lossless full-fidelity transport codec / presenter split.
- No new NATS-backed regression test in `user-service/historyclient` (that package has no
  test harness today); the type- and model-layer tests cover the root cause and the reported
  path.

## 5. Risks & mitigations

- **Client-contract / sonic wire regression.** Adding the `DisplayName` preference to
  `MarshalJSON` must not change output for Cassandra-read messages. Mitigation: `DisplayName`
  is empty on that path, so the composed value is used — output is byte-identical. The
  existing sonic wire-compat tests (`broadcast-worker`, `message-gatekeeper`) and client-api
  examples act as guards.
- **Cassandra UDT binding.** The new field must not perturb gocql. Mitigation: `cql:"-"`
  excludes it from bind/scan; message-worker's `buildCassandraMessage` and history-service's
  cassrepo scan never set it. Verify compilation and existing integration tests.
- **sonic now decodes `Reactions`.** Adding `UnmarshalJSON` makes the type sonic-decodable
  too; this is additive and changes no existing behaviour (the projections still decode their
  narrow shapes). No action required.

## 6. Verification

- `make test SERVICE=pkg/model` (type + model unit tests, `-race`).
- `make lint` — clean.
- `make sast` — clean (change is additive stdlib JSON; no `errcode`/`WithCause`/unsafe
  patterns).
- Confirm `message-gatekeeper` and `broadcast-worker` tests (sonic wire-compat) still pass.
