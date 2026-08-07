# Thread List reply count (`thread.list` → `ThreadListItem`)

**Date:** 2026-08-07
**Status:** Approved — implemented

## Scope

Return the number of thread replies on each row of the cross-site thread inbox:

- Client-facing: `chat.user.{account}.request.user.{siteID}.thread.list`
  (`user-service/service/threads.go:54`, `ListUserThreads`)
- Per-site leaf: `chat.server.request.thread.{siteID}.subscription.list`
  (`history-service/internal/service/threads.go:164`, `ListThreadSubscriptions`)

Not in scope: `msg.thread.parent` (`GetThreadParentMessages`) and `msg.thread`
(`GetThreadMessages`) already return full `Message` structs, so `tcount` is a
first-class field there.

## Current implementation

```
client
  └─ user-service  ListUserThreads                       threads.go:54
       ├─ fan-out over ALL_SITE_IDS                      threads.go:157
       │    └─ historyclient.GetThreadList               historyclient/client.go:32
       └─ merge + sort (lastMsgAt, threadRoomId) DESC    threads.go:97
            └─ enrichThreadPage (roomName / hrInfo only) threads.go:230

history-service  ListThreadSubscriptions                 threads.go:164
  ├─ Mongo keyset page                                   mongorepo/threadsubscription.go:69
  │    thread_subscriptions ⋈ subscriptions ⋈ thread_rooms   mongorepo/pipelines.go:56
  │    → ThreadSubRow{threadRoomId, roomId, siteId, roomName, roomType,
  │                   parentMessageId, lastMsgId, lastMsgAt, lastSeenAt, hasMention}
  └─ buildThreadItems                                    threads.go:203
       └─ GetMessagesByIDs(parents ∪ lasts)              cassrepo/messages_by_id.go:44
            SELECT … tcount … FROM messages_by_id        cassrepo/messages_by_room.go:13
```

There is **no reply-count field**. `ThreadListItem`
(`pkg/model/threadlist.go:8-36`) carries 13 fields and the count is not one of
them. The count is only reachable *inside* the hydrated parent body:

```go
// pkg/model/threadlist.go:21-23
// LastMsgAt is the thread's last-activity time (UTC ms) and the global sort
// key for the inbox. Reply count rides on ParentMessage.TCount.
LastMsgAt int64 `json:"lastMsgAt" bson:"lastMsgAt"`
```

This was a deliberate decision at design time
(`docs/design/user-thread-list.md:328-329`: "Reply count is **not** a separate
field — it already rides on `parentMessage.TCount`; clients read it there") and
it does work end-to-end today — `history-service/internal/service/threadlist_test.go:87-88`
decodes the opaque parent and asserts `*TCount == 4`.

`tcount` itself is sound: it is the bounded, soft-delete-aware count of
`thread_messages_by_thread` rows, written by the two authoritative writers
(message-worker on reply-add, history-service on reply-delete) through the
shared `pkg/threadcount` helper, and it is a real column on `messages_by_id`
(`docker-local/cassandra/init/13-table-messages_by_id.cql:12`).

So the data is present. What is missing is the **contract**.

## What's wrong

**1 — The count is buried in a payload the RPC deliberately refuses to parse.**
`ParentMessage` is `json.RawMessage` (`pkg/model/threadlist.go:29`):
history-service emits it pre-marshaled and user-service forwards it verbatim,
**never decoding it**, because `cassandra.Message.Reactions` is a struct-keyed
map with no JSON decoder (`pkg/model/threadlist.go:25-28`). Consequences:

- No stage of the server pipeline can read, validate, log, or sort on the count.
- Any Go consumer (loadgen, a sibling service, an integration test) needs a
  narrow projection decode to get a number the response already knows —
  the same workaround `message-gatekeeper/fetcher_history.go` needed.
- The thread-list contract doesn't own the field it depends on: the name,
  type, and presence rules of the count are governed by the `Message` schema.

**2 — Both hops of the carrier are declared optional.**
`docs/client-api.md:5318` marks `parentMessage` *Optional*; `docs/client-api.md:2701`
marks `tcount` *Optional*. So per the published contract, the count may legally
be absent from any row. A thread-inbox row exists only because the thread has at
least one reply, so "count absent" is never a meaningful state for this RPC —
yet every client must code the `undefined` branch.

**3 — And it genuinely is absent for migrated threads.**
`data-migration/oplog-collections-transformer` writes `thread_rooms` and
`thread_subscriptions` straight into Mongo (`targetstore.go:31`, `threadsubs.go`)
and never touches Cassandra. Those threads appear in the inbox with a parent
whose `tcount` column was never written to Cassandra; `*int` + `omitempty`
(`pkg/model/cassandra/message.go:89`) then omits the key entirely.
`data-migration/README.md:71` already books this as an accepted limitation
("Migrated threads/rooms may show stale/zero counts"). The client cannot
distinguish *unknown* from *zero*.

**4 — The badge semantics are undocumented at this level.**
`tcount` is capped: `pkg/threadcount.Cap = 99`, and `CountAndLatest` stops
counting there, so 99 means "99 or more". That is documented once on the generic
Message schema (`docs/client-api.md:2701`) and nowhere in the List User Threads
section, so a reader of that section has no reason to expect a ceiling.

**5 — Nothing pins the count across the aggregator.**
The single assertion lives in the history-service leaf test
(`threadlist_test.go:87-88`). No user-service test asserts the count survives the
merge/sort/enrich pass, and `pkg/model/threadlist_test.go` never round-trips one.

## The fix

Surface the count as an explicit, always-present field on the item, sourced from
the parent that `buildThreadItems` **already hydrates**. Zero extra reads, zero
new writes, no new source of truth.

### 1. `pkg/model/threadlist.go`

```go
// TCount is the thread's non-deleted reply count, capped at
// pkg/threadcount.Cap — 99 means "99 or more". Same number and same name as
// Message.tcount, lifted to the row so clients need not parse ParentMessage.
// 0 when the parent's column was never written (migrated threads). Always
// present on the wire so clients never branch on undefined.
TCount int `json:"tcount" bson:"tcount"`
```

No `omitempty` — a zero count must serialize. Note this is a plain `int`, not
the `*int` of `cassandra.Message.TCount`: at the row level the count is
unconditional. See open question 2 for the trade-off.

Update the `LastMsgAt` comment (line 21-23) to stop pointing at
`ParentMessage.TCount` as the carrier.

### 2. `history-service/internal/service/threads.go` — `buildThreadItems`

In the item literal (currently lines 227-237), after the parent is known to have
hydrated:

```go
if parent.TCount != nil {
    item.TCount = *parent.TCount
}
```

`parent` is already in hand at line 222; nothing else moves.

### 3. `user-service` — no code change

`ListUserThreads` copies items verbatim (`threads.go:93`) and `enrichThreadPage`
only rewrites `RoomName` / `HRInfo`, so the field rides through untouched. It
still needs a test (below) to keep it that way.

`parentMessage.tcount` stays exactly as it is — this is purely a surfacing
change, and removing it would be a breaking change for anything already reading
it.

## TDD steps

Red first, per CLAUDE.md §4.

1. `pkg/model/threadlist_test.go` — round-trip `TCount`; assert the key is
   present in the marshaled JSON when the value is `0`.
2. `history-service/internal/service/threadlist_test.go` — table-driven over the
   parent's `TCount`: `intPtr(4)` → `4`; `nil` → `0`; `intPtr(0)` → `0`;
   `intPtr(99)` → `99` (cap passes through unchanged).
3. `user-service/service/threads_test.go` — `TCount` survives the **whole**
   aggregator pass: the cross-site merge, the `(lastMsgAt, threadRoomId) DESC`
   sort, and `enrichThreadPage`. The aggregator copies items verbatim today, so
   this test is what keeps it that way.

   One page, rows from **two different sites** with interleaved `lastMsgAt` so
   the sort genuinely reorders them, and one row per `enrichThreadPage` branch
   (`user-service/service/threads.go:230-248`) — each branch mutates the item
   differently, so each needs its own assertion that `TCount` is untouched:

   | Row | `roomType` | What enrichment does to it | Assert |
   |---|---|---|---|
   | A | `channel` | nothing — no per-row enrichment | `TCount` intact |
   | B | `dm` | sets `HRInfo` from the HR lookup | `TCount` intact **and** `HRInfo` set |
   | C | `botDM` | overwrites `RoomName` with the app name | `TCount` intact **and** `RoomName` rewritten |

   Assert on the final `resp.Items` — indexed by the post-sort order, not the
   per-site input order — so a regression that drops the field during the merge,
   the sort, or any one enrichment branch fails distinguishably.

   Plus two degraded cases, since enrichment failures are swallowed by design
   (`lookupThreadHRInfo` / `lookupThreadApps` return nil on error): the HR
   lookup failing and the app lookup failing must each still leave `TCount`
   intact on every row.
4. Implement 1–2 above; `make test SERVICE=history-service`,
   `make test SERVICE=user-service`, `make lint`.

## Docs (same PR — CLAUDE.md §5)

- `docs/client-api.md` — add a `tcount` row to the **ThreadListItem** table
  (§ List User Threads) stating the 99 cap, that it is always present, and that
  `0` covers "never counted"; add it to the JSON example; soften the
  `parentMessage` note so the count is no longer described as riding there.
- `docs/client-api/request-reply.md` — **no change needed**: the derived view
  (line 1813-1815) links to the canonical `ThreadListItem` schema rather than
  restating it.
- `docs/client-api/events.md` — no change (`thread.list` emits no events).
- `docs/design/user-thread-list.md:328-329` — supersede the "not a separate
  field" decision with a pointer to this spec.

## Alternatives considered

**B — maintain a `tcount` on the `thread_rooms` Mongo doc.**
message-worker increments on reply-add, history-service recomputes on
reply-delete, and `userThreadSubscriptionsPipeline` projects it out of the
existing `tr` `$lookup` (`pipelines.go:92-102`) — no Cassandra dependency for the
inbox at all, and no 99 cap. Rejected for this change: it adds a write to the
reply hot path, creates a second source of truth that can drift from `tcount`,
and needs a backfill across every existing `thread_rooms` doc. Revisit only if
exact counts above 99 become a hard product requirement.

**C — count per row from Cassandra at read time.**
One bounded `thread_messages_by_thread` partition scan per item, up to 100 per
page per site. Rejected on cost; it is precisely what the cached `tcount` exists
to avoid.

## Legacy-repo suggestions — reviewed

Three suggestions came back from the legacy monolith. Two apply; one does not
survive the monolith → services split.

**"tcount lives on the Message document (Cassandra `messages_by_id.tcount`)" —
confirmed, no change.** Matches the trace above. Worth naming the difference:
in the monolith the count sat on a **Mongo** message document, so it was in the
same database as the subscription rows. Here messages are Cassandra-only, which
is exactly what breaks the next suggestion.

**"The pipeline should `$lookup` the parent message at aggregation time, then a
`TransformToThreadSubscription` step converts it" — right shape, wrong
mechanism here. The equivalent already exists in code.**

In tchat2 the parent lived in the same MongoDB as the subscription rows, so
joining it inside the aggregation was the natural move. **Here the parent's
`tcount` lives in Cassandra (`messages_by_id`), so the join cannot happen in
Mongo at all** — the aggregation returns row *keys* and the join happens in Go,
one hop later:

```
mongo agg  → ThreadSubRow{parentMessageId, lastMsgId, …}   pipelines.go:56
             ↓ threadListLookupMsgIDs — dedup parents ∪ lasts
cassandra  → GetMessagesByIDs(ids)                         cassrepo/messages_by_id.go:44
             ↓
             buildThreadItems → ThreadListItem[]           threads.go:203
```

- *Both halves already exist.* `GetMessagesByIDs` is an existing Cassandra
  batch read, and `buildThreadItems` is our `TransformToThreadSubscription` —
  it takes the `ThreadSubRow`s plus the hydrated parents and emits
  `ThreadListItem`s. The parent **is** already resolved in the same request, so
  the count is free. This change adds no read; it only lifts a value the
  transform already holds.
- *The `$lookup` itself cannot port.* There is no production Mongo `messages`
  collection to join to. The only `db.Collection("messages")` in the repo is
  `tools/seed-sample-data/mongo.go:122`, a dev seeder no service reads;
  history-service's Mongo repos bind `apps`, `subscriptions`, `rooms`,
  `thread_rooms`, `thread_subscriptions` and nothing else. Mongo cannot
  `$lookup` across engines. Secondary point: CLAUDE.md bans `$lookup` without a
  documented justification, and `userThreadSubscriptionsPipeline` already
  carries two justified ones.
- *The in-code join is the more efficient one.* The legacy pipeline resolved a
  parent **per row**; we resolve the **whole page in one deduped batch** —
  `threadListLookupMsgIDs` unions parents ∪ last-messages and drops duplicates
  (a message that is one row's parent and another's last message is fetched
  once), then `GetMessagesByIDs` fans the union out as token-aware
  single-partition point reads with at most `maxConcurrentIDReads = 16` in
  flight — deliberately not a multi-partition `IN` scatter. So the page costs
  one bounded fan-out, not N sequential joins.
- The nearest true port of "get the count from the aggregation" is denormalizing
  it onto `thread_rooms` — that is Option B below, and it is not a minor fix.

**"Stay with `tcount` instead of `replyCount`" — accepted, sketch updated.**
The whole ecosystem is already `tcount`-shaped: `docs/client-api.md:2701`,
`chat-frontend/src/api/types.ts:239,281` (`tcount?: number`, "surfaced as the
'💬 N replies' badge"), the optimistic bumps in
`chat-frontend/src/context/RoomEventsContext/reducer.js:375,802`, and the
`newTcount` broadcast field (`pkg/model/event.go:38,380`). A second name for the
same number buys nothing. The frontend type is already optional (`tcount?`), so
an always-present item-level `tcount` is additive, not breaking.

## Open questions

1. **Is the 99 cap acceptable for the inbox badge?** If product wants exact
   counts, this becomes Option B and is no longer a minor fix.
2. **`int` or `*int` for the item-level `tcount`?** Proposed: plain `int`,
   always serialized — that is the main ergonomic win, and
   `data-migration/README.md:71` already accepts zero counts for migrated
   threads. The honest cost is that a migrated thread with replies reports
   `0`, indistinguishable from a thread whose replies were all deleted. `*int`
   + `omitempty` keeps that distinction but reinstates the `undefined` branch
   this change exists to remove. **Resolved: plain `int`.**
3. **Do we backfill migrated threads?** Out of scope here; it would be a
   one-off job recomputing `tcount` per `thread_rooms` row via
   `pkg/threadcount`. Worth a follow-up ticket rather than blocking this.
