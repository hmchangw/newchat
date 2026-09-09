# Is PR #477 (`errcode.FromReply`) required?

Analysis of whether the hand-rolled `errcode.Parse(...) + Code.Valid()` idiom is
actually broken, given that every peer in this system is a service we build and
deploy ourselves.

Reproduce with:

```
make test SERVICE=broadcast-worker
go test ./pkg/errcode/ -run XXX -bench 'Parse_|FromReply_' -benchmem
```

## 1. The premise holds: no producer here can emit a malformed envelope

`TestErrorEnvelope_ProducersAreAlwaysWellFormed` drives every server-side
error-reply path through its real marshaller and inspects the bytes.

| producer | bytes on the wire |
|---|---|
| `errnats.Marshal` (all 8 constructors) | `{"code":"<canonical>","error":"…"}` |
| `errnats.Marshal(ctx, errors.New("boom"))` | `{"code":"internal","error":"internal error"}` |
| `errnats.MarshalQuiet` | `{"code":"internal","error":"x"}` |
| `errnats.OversizeEnvelope` | `{"code":"internal","reason":"response_too_large","error":"…"}` |
| `natsutil.ReplyJSON` marshal-failure fallback | `{"code":"internal","error":"internal error"}` |
| `errcode.WithMetadata` | `{"metadata":{"retryAfter":"5"}}` — always `map[string]string` |

Three language-level facts close the set:

- `errcode.New` panics on a non-canonical `Code`, so `Code.Valid()` is true for
  every constructible `*Error`.
- `errcode.New` panics on an empty message, so `"error"` is always a non-empty string.
- `WithMetadata(kv ...string)` is the only way to populate `Metadata`, so its
  values are always JSON strings.
- There are **no `errcode.Error{…}` struct literals** anywhere outside the package.

`TestFetchParent_RelaysEveryProducibleEnvelope` then replays each of those
envelopes through the real `broadcast-worker` call site over a real NATS
connection. All of them relay as a typed `*errcode.Error`. **The existing code
is correct for every payload this fleet can produce.**

## 2. The three failure shapes in the PR need a peer built from different source

`TestFetchParent_FailsOpenOnUnproducibleEnvelopes` confirms the fail-opens are
real — and records what each one costs to reach:

| payload | old result | precondition |
|---|---|---|
| `{"code":"quota_exhausted","error":"…"}` | `err == nil`, zero-value parent | a peer whose `category.go` declares a 9th `Code` |
| `{"code":"unavailable","error":"…","metadata":{"retryAfter":5}}` | `err == nil`, zero-value parent | a peer whose `Error.Metadata` is `map[string]any` |
| `{"error":"message not found"}` | `err == nil`, zero-value parent | a peer replying outside `errnats`/`Classify` |

Each precondition is an edit to `pkg/errcode` itself, deployed on one side of an
RPC and not the other. The `Code` set is 8 HTTP categories and has not changed
since the package landed.

## 3. The evolution that *is* routine already works

`Reason` is the open set — 15 `codes_*.go` files, extended in most feature PRs —
and it is never validated. `TestFetchParent_ReasonSkewAlreadyWorks` shows a
reason this build has never seen relaying correctly through the existing code,
code and reason intact. **Version skew in the field means new reasons, and the
current idiom handles them.**

## 4. The "three unmarshals" cost is not on the hot path

| | ns/op | B/op | allocs/op |
|---|---|---|---|
| `Parse` success (hot path) | 3562 | 280 | 5 |
| `FromReply` success (hot path) | **3522** | **224** | 5 |
| `Parse` error | 1755 | 360 | 8 |
| `FromReply` error | 3317 | 808 | 17 |

On a success reply `FromReply` decodes one `json.RawMessage` field where `Parse`
decodes the whole `Error` struct, so it is marginally *faster* and allocates
less. The extra passes only run on an error reply, which has already spent a
failed network round trip. **Perf is not an argument against the PR.**

## 5. What FromReply does not do

`TestFromReply_IgnoresNonJSON`: a non-JSON reply returns `nil`. Every caller
still needs its own decode after the envelope check — which is why the PR had to
give `ClearAllThreadUnread` a decoder separately.

## Verdict

The three failure shapes are real but require a peer built from source that
differs from this build inside `pkg/errcode`. Against payloads the fleet can
actually emit, the existing call sites are correct, and the routine skew case
(new `Reason`) already works.

What the change is worth on its own merits, independent of the malformed-payload
argument:

- **21 hand-written sites in two different shapes.** 6 use `ok && Code.Valid()`,
  15 use bare `ok`; the two disagree on the same bytes. One helper is easier to
  read than either.
- **`peer_client.go`'s `%s` → `%w`** is a real bug fix — the old form broke the
  error chain — and needs none of the rest of the PR.
- **The loadgen/soak sites** hold to a stricter bar than production code: a
  harness whose job is to detect breakage must not score an unreadable reply as
  a served request.

Against that: `pkg/errcode/exemptions_contract_test.go` (210 lines of AST
scanning) plus a semgrep rule is enforcement machinery aimed at the
malformed-payload class specifically, which is the part the evidence above does
not support.
