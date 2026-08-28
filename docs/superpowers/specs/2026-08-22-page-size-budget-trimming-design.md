# Proactive page-size trimming for paginated client RPCs

Date: 2026-08-22
Status: approved for implementation

## Problem

PR #350 made an oversize reply return `internal` / `response_too_large` instead
of timing out. That reports the failure; it does not deliver the page. The
client must still retry with a smaller `limit`, and today's frontend does so by
halving `size` on each rejection — a wasted round trip per halving, and a
visible stall on exactly the busiest rooms.

No service bounds its reply against the broker's `max_payload`. `history-service`
has no size guard at all: it marshals whatever Cassandra returned. `room-service`
is the only service with a proactive cap (`marshalBounded`), and it only rejects —
it never trims.

The fix is to size the page to the transport before replying, so a paginated read
returns a smaller page with the "more" flag set rather than an error.

## Goals

- A paginated reply always fits under `max_payload`.
- Trimming never loses or duplicates a row: the next page resumes exactly at the
  first dropped row.
- The frontend's `size/2` retry never engages for the RPCs in scope.
- Forward progress is guaranteed even when one row alone exceeds the budget.

## Non-goals

Out of scope, unchanged, still relying on #350's `response_too_large` fallback:

- `LoadNextMessages`, `ListPinnedMessages`, `GetThreadMessages` — their
  `nextCursor` is Cassandra's opaque driver `PageState`, which marks the position
  after the last row **the driver fetched**. It cannot be re-derived from row
  content, so trimming a page without re-deriving it would silently skip the
  dropped rows. Fixing these requires either value-based cursors or adaptive
  fetch sizing; both are larger changes deferred to a follow-up.
- `search-service` (`offset`/`size` paging) — trimming shifts the meaning of the
  client's next `offset`.

The frontend's `size/2` retry therefore stays in place for those.

## Scope

Three RPCs whose next-page position is derived from returned content, so
trimming is provably safe:

| RPC | Service | Next page located by | Trim |
|---|---|---|---|
| Load History | history-service | client re-asks `before` = oldest returned `createdAt` | drop from tail |
| Thread List | user-service | composite value cursor `{lastMsgAt, threadRoomId}` | drop from tail |
| Load Surrounding | history-service | `moreBefore` / `moreAfter`, client pages outward by timestamp | drop from both ends |

## Design

### `pkg/pagefit`

A new package owning size arithmetic only. It knows nothing about messages,
threads, or NATS, and is unit-testable without any of them.

```go
// Budget is the byte ceiling one reply must fit under.
type Budget struct{ /* max int */ }

// NewBudget derives the ceiling from the broker's advertised max_payload,
// less a reserve for headers and the non-item envelope fields.
func NewBudget(brokerMaxPayload int64, reserve int) Budget

// Prefix returns the largest n such that items[:n] fits the budget.
// Returns 1 for a non-empty slice whose first item alone overflows, so the
// caller can always make forward progress.
func Prefix[T any](items []T, b Budget, envelope int) int

// Window returns the [lo,hi) span around pivot that fits, grown symmetrically
// outward from pivot. Always includes pivot.
func Window[T any](items []T, pivot int, b Budget, envelope int) (lo, hi int)
```

Sizing marshals each item once and prefix-sums the encoded lengths plus JSON
separators. The caller then does one exact check on the assembled response and
drops one more row if the envelope pushed it over — a bounded loop, not a retry.

The alternative (estimating from a running average of row size) avoids the
second marshal pass but can guess wrong and overflow anyway, which is the bug
being fixed. Correctness wins; the cost is one extra marshal of at most
`maxPageSize` (100) rows on a read RPC, and is covered by a benchmark.

### Budget derivation

Each service reads the broker's cap at startup via `nc.NatsConn().MaxPayload()`
and injects it into its handler, mirroring `room-service/main.go:318`.

`MAX_RESPONSE_BYTES` (env, `envDefault:"0"`) overrides it; `0` means derive from
the broker. A non-positive resolved budget disables trimming, so the feature can
be turned off without a rollback.

### Trim semantics

**Load History** returns messages DESC (newest first) and the client re-asks with
`before` = the oldest returned `createdAt`. Dropping from the tail therefore
leaves the oldest *kept* message as the client's next `before`, and the next page
resumes exactly at the first dropped row. `hasNext` is forced true when any row
was dropped.

The existing invariant holds and is load-bearing: an empty page must never claim
`hasNext`, because the client would have no `createdAt` to page from
(`history-service/internal/service/messages.go:96`). `Prefix` guaranteeing a
minimum of 1 preserves it.

**Thread List** already merges, sorts DESC, trims to `limit`, and re-derives
`NextCursor` from the last kept item. The byte-trim slots in as a second trim and
the existing cursor derivation picks up the right item unchanged.

Ordering matters: `enrichThreadPage` runs after the count-trim and *adds* bytes
(display names, HR records). The byte-trim must therefore run **after**
enrichment, with the cursor re-derived from the post-trim last item. Trimming
before enrichment would overflow again.

**Load Surrounding** assembles ASC as `[reversed before] + [central] + [after]`.
`Window` keeps the central message and grows outward, so dropping from the front
sets `moreBefore` and from the back sets `moreAfter`. In `timestamp` mode there
is no central message and the pivot is the insertion index.

### Single-row backstop

When one row alone exceeds the budget, it is returned blanked rather than
dropped or errored, so pagination advances past it.

Blanked: `msg`, `mentions`, `attachments` (and `decodedAttachments`), `card`,
`cardAction`, `quotedParentMessage`, `reactions`, `sysMsgData`.

Kept: all identifiers, `sender`, `createdAt`, `type`, and thread/edit metadata.

`type` is deliberately retained — the frontend needs it to choose a placeholder.
This differs from `redactUnavailablePins`, which also clears `type` because a
pre-access system message would otherwise leak event details past the
placeholder. That reasoning does not apply here: the caller is authorised to see
this message, it is merely too large to ship inside a page.

A new `truncated bool` field on `cassandra.Message` (`json:"truncated,omitempty"`)
marks the row. It distinguishes "too large to send here" from the access-window
redaction, which sets `msg` to `"This message is unavailable"` and carries no
flag — different meaning, different client recovery.

Why this case is a backstop and not a hot path: `content` is capped at 20 KB
(`message-gatekeeper/handler.go:332`) and attachments at 1 x 8 KB
(`MAX_ATTACHMENTS=1`, `MAX_ATTACHMENT_BYTES=8192`), so a message's realistic
ceiling is ~50 KB against a 128 KiB budget. Only `reactions` is unbounded, and
overflowing on it alone needs thousands of reactors on one message. The backstop
exists so that case degrades visibly instead of dead-ending the client, not
because it is expected.

## Client-facing contract

`truncated` is a new field on a client-facing response struct, so the same PR
updates `docs/client-api.md` and both derived views
(`docs/client-api/request-reply.md`, `docs/client-api/events.md`).

Documented behaviour: a paginated reply may return fewer rows than `limit` even
when more exist; the "more" flag (`hasNext` / `moreBefore` / `moreAfter`) is
authoritative, and `limit` is a maximum, never a guarantee. Clients must page
until the flag clears rather than assuming a short page means the end.

## Testing

TDD throughout; every test watched failing for the right reason first.

**`pkg/pagefit` unit** — empty slice, exact fit, one byte over, every row
oversize, single row oversize (returns 1), window symmetry, pivot at either edge,
disabled budget. Plus a benchmark for the marshal pass at 100 rows.

**Handler unit (mocked stores)** — for each of the three RPCs: an oversize page
trims and sets its more-flag; a page that fits is untouched; the single-oversize-row
case returns one blanked row with `truncated:true` and the retained fields intact.

**Pagination continuity** — the test that guards the actual risk: take a trimmed
page 1, feed its own cursor/`before` back in, and assert page 2 begins exactly at
the first dropped row, with the union across pages equal to the full set, each row
appearing exactly once.

**Integration** — real NATS at a low `max_payload` with real Cassandra/Mongo,
walking a seeded room across several trimmed pages and asserting the union equals
the seeded set exactly once, and that `response_too_large` never fires.

## Risks

- **Second marshal pass** adds CPU on read RPCs. Bounded at 100 rows, benchmarked,
  and skipped entirely when the budget is disabled.
- **Thread List enrichment ordering** is the one place a careless change
  reintroduces the overflow. Called out in code comments and pinned by a test that
  enriches rows to just over the budget.
- **A short page is a contract change in practice** even though it is legal today.
  Clients that assumed `len(items) == limit` implies more, or that a short page
  means the end, need the documented rule above.
