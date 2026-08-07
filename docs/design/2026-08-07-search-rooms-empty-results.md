# search.rooms returns `[]` — investigation and resolution sketch

**Symptom:** `chat.user.{account}.request.search.{siteID}.rooms` replies `200` with
`{"rooms": []}` on the test server.

**Status:** code-level investigation complete; no test-server access from this session,
so the ranked hypotheses below are *candidates with a discriminating check each*, not a
confirmed root cause. §2 is the runbook that turns them into one answer.

---

## 0. What the empty reply already rules out

`[]` is not `null` and not an error, which pins down where the failure is *not*:

- `parseRooms` builds `make([]model.SearchRoom, 0, …)` (`search-service/response.go:107`),
  so an empty slice serializes to `[]`. A nil reply would have been `null`.
- The RPC reached search-service, the account param resolved, pagination and the
  non-empty-query guard passed (`search-service/handler.go:131-145`), the ES call
  returned `200`, and the response parsed.

So search-service, NATS routing, and Elasticsearch reachability are all healthy.
**The spotlight query genuinely matched zero documents.**

## 1. Why every possible cause produces the identical symptom

This is the reason the bug is hard to read, and it should be fixed regardless of which
hypothesis wins.

```go
// pkg/searchengine/adapter.go:304
path := fmt.Sprintf("/%s/_search?ignore_unavailable=true&allow_no_indices=true", …)
```

```go
// search-service/main.go:115 — a wildcard, so version rollover is transparent
spotlightReadPattern := fmt.Sprintf("%s-*", spotlightBase)
// search-service/handler.go:162
raw, err := h.store.Search(ctx, []string{h.cfg.SpotlightReadPattern}, body)
```

A **wildcard** index pattern combined with `allow_no_indices=true` means a
*non-existent index*, a *misnamed index*, an *empty index*, an *unindexed field*, and a
*genuine zero-match* are **all** `HTTP 200` with `hits: []`. No error, no log line, no
metric distinguishes them. The client cannot tell "you have no matching rooms" from
"the index this service reads has never existed".

The wildcard is deliberate and correct (it is what makes the `-v1 → -v2` reindex
rollover seamless). The missing piece is that nothing observes the difference.

## 2. Triage runbook — run this first, on the test server

Each step maps to a hypothesis in §3. Substitute the site's real values.

```bash
# 2.1 Does the index search-service reads actually exist, and does it hold anything?
curl -s "$ES/_cat/indices/spotlight-*?v"
#   0 rows          -> H6 (config drift) or H7 (writer dead)
#   rows, docs.count=0 -> H7
#   rows, docs.count>0 -> continue

# 2.2 Do the two services agree on the index name? (NOTE: different env var names!)
kubectl exec deploy/search-service     -- printenv SEARCH_SPOTLIGHT_INDEX
kubectl exec deploy/search-sync-worker -- printenv SPOTLIGHT_INDEX
#   different bases -> H6 confirmed

# 2.3 Is the writer alive and caught up?
kubectl logs deploy/search-sync-worker --tail=50 | grep -E 'collection wired|create consumer failed'
nats consumer info "INBOX-$SITE" spotlight-sync   # num_pending should not climb
nats stream ls | grep -i inbox                    # INBOX-site vs INBOX_site  -> H7
#   CrashLoopBackOff / "create consumer failed" -> H7 confirmed

# 2.4 Does a document for this exact account exist at all?
curl -s "$ES/spotlight-*/_search?size=3" -H 'Content-Type: application/json' -d '{
  "query":{"term":{"userAccount":"'"$ACCOUNT"'"}}}' | jq '.hits.total, .hits.hits[]._source'
#   total=0 but 2.1 showed docs -> H5 (mapping) or account-case mismatch; try:
curl -s "$ES/spotlight-*/_search?size=3" -H 'Content-Type: application/json' -d '{
  "query":{"match":{"userAccount":"'"$ACCOUNT"'"}}}' | jq '.hits.total'
#   match finds it but term does not -> H5 confirmed (userAccount is `text`, not `keyword`)
#   docs present with roomName:"" -> H1 confirmed

# 2.5 Is it the analyzer? Replay the real query, then relax it.
curl -s "$ES/spotlight-*/_analyze" -H 'Content-Type: application/json' -d '{
  "analyzer":"custom_analyzer","text":"<a real room name from 2.4>"}' | jq '.tokens[].token'
#   one token for a multi-word / CJK name -> H4 confirmed

# 2.6 What mappings does the live index actually have?
curl -s "$ES/spotlight-*/_mapping" | jq '.[].mappings.properties'
#   userAccount/roomName as `text` with a `.keyword` subfield = ES dynamic defaults,
#   i.e. the template never applied -> H5 confirmed
```

## 3. Ranked hypotheses

### H7 — the writer is dead: INBOX stream renamed underscore → hyphen  ·  *most likely recent regression*

`6685001` ("Rename JetStream stream names from underscore to hyphen format", #181,
3 days ago) renamed `INBOX_{siteID}` → `INBOX-{siteID}`. search-sync-worker is a **pure
consumer** of INBOX and never bootstraps it — the skip is explicit and applies even when
`BOOTSTRAP_STREAMS=true`:

```go
// search-sync-worker/main.go:249-256
inboxName := stream.Inbox(cfg.SiteID).Name
if cfg.Bootstrap.Enabled && streamCfg.Name != inboxName && streamCfg.Name != hrName {
```

and consumer creation is fatal:

```go
// search-sync-worker/main.go:290-298
cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, consumerCfg)
if err != nil { slog.Error("create consumer failed", …); os.Exit(1) }
```

If the test server's NATS still carries `INBOX_{siteID}` (ops/IaC-owned, not redeployed
with the code), the worker crash-loops on startup and **nothing has reached the spotlight
index since the rename**. It fits "worked before, empty now" better than any other
hypothesis, and it is the cheapest to check (2.3).

**Discriminating check:** 2.3. **Fix:** ops-side — rename the stream, or run both names
during migration. Code change needed: see F1 (fail visibly, not silently).

### H1 — every DM is indexed with an empty `roomName`  ·  *confirmed in code*

`resolveRoomName` returns `""` for anything that is not a channel:

```go
// room-worker/handler.go:1335-1340
func resolveRoomName(req *model.CreateRoomRequest, roomType model.RoomType) string {
	if roomType == model.RoomTypeChannel { return req.Name }
	return ""
}
```

That empty name is what gets published and then indexed verbatim:

- `finishCreateRoom` → `MemberAddEvent{RoomName: room.Name}` (`room-worker/handler.go:1796`)
- `publishSyncDMInbox` → `RoomName: ""` hardcoded (`room-worker/handler.go:2388`)
- `newSpotlightSearchIndex` → `RoomName: evt.RoomName` (`search-sync-worker/spotlight.go:106`)

A document with `roomName: ""` can never match a `multi_match` on `roomName`
(`search-service/query_rooms.go:44-49`). **`roomType: "dm"` searches always return `[]`**,
and DMs are silently absent from `roomType: "all"`.

The backfill does **not** have this bug — it reads the per-subscription name:

```go
// data-migration/es-index-migrator/spotlightaction.go:30
RoomName:    sub.Name,
```

and `buildDMSubs` names each subscription after the counterpart
(`room-worker/handler.go:1344-1349`). So the two writers of the same index disagree: DMs
are searchable immediately after a backfill and unsearchable for every DM created
afterwards. That alone can look like "it used to work".

### H2 — same-site server-created DMs publish no INBOX event at all  ·  *confirmed in code*

```go
// room-worker/handler.go:2381-2384
func (h *Handler) publishSyncDMInbox(…) error {
	if other.SiteID == "" || other.SiteID == h.siteID { return nil }
```

`serverCreateDM` (`room-worker/handler.go:1936`, the sync path) publishes **only** a
federated event to the remote site (`h.federate(…, other.SiteID, …)`,
`room-worker/handler.go:2409`). There is no `InboxInternal` publish on this path at all,
so:

- same-site DM → neither participant gets a spotlight document;
- cross-site DM → only the *remote* member gets one (and with `roomName: ""`, per H1);
  the local requester gets nothing.

Contrast the user-initiated path (`room-worker/handler.go:1817`), which does publish
`subject.InboxInternal(room.SiteID, model.InboxMemberAdded)`.

### H3 — `room_renamed` never updates spotlight  ·  *confirmed in code*

The consumer filters on member events only:

```go
// pkg/subject/subject.go:296-304 (via search-sync-worker/inbox_stream.go:36)
InboxInternal(siteID, "member_added"), InboxInternal(siteID, "member_removed"),
InboxExternal(siteID, "member_added"), InboxExternal(siteID, "member_removed"),
```

`inbox-worker` handles `room_renamed` for Mongo (`inbox-worker/handler.go:365`);
search-sync-worker has no equivalent. A renamed channel keeps its **old** name in
spotlight forever, so searching the name users actually see returns `[]`. If the test
server's rooms were renamed after seeding, this reproduces the report exactly.

### H4 — the production analyzer is not the one any test exercises  ·  *confirmed coverage gap*

Production maps `roomName` as `search_as_you_type` with `custom_analyzer`
(`pkg/searchindex/template.go:156` via `SpotlightDoc`'s `es:"search_as_you_type,custom_analyzer"`),
where `custom_analyzer` is **whitespace tokenizer + lowercase** and nothing else
(`pkg/searchindex/template.go:135-152`).

Every integration test that runs a *real query* against a *real ES* builds the index by
hand, **without an analyzer** — so it silently gets the `standard` analyzer:

```go
// search-service/integration_rooms_test.go:71-74
"roomName": map[string]any{
	"type": "search_as_you_type",
},
```

The tests that *do* assert `custom_analyzer` (`pkg/searchindex/template_test.go:167-168`,
`search-sync-worker/spotlight_test.go:113-114`) only check the JSON shape — they never
push a document through it. **Nothing in the suite covers the seam that is failing.**

Behavioural consequences of whitespace-vs-standard:

- `standard` splits `engineering-announcements` into `[engineering, announcements]`;
  `custom_analyzer` yields the single token `engineering-announcements`. With
  `match_bool_prefix` a *leading* prefix still matches, but `"announcements"` does not.
- **CJK is the sharp edge.** The messages template includes `cjk_bigram`
  (`pkg/searchindex/template.go:81`); the spotlight template does **not**. A Chinese room
  name is one indivisible token, so any query that is not a left-anchored prefix of the
  whole name returns `[]`. Given the product indexes `sectTCName` / `deptTCName`, CJK
  room names should be assumed present on the test server.

### H5 — the live index may predate its template, leaving ES dynamic mappings

`UpsertTemplate` targets `_index_template` (`pkg/searchengine/adapter.go:193`), which
applies **only at index creation**. Unlike the messages collection, spotlight has no
startup mapping push — it inherits the no-op:

```go
// search-sync-worker/inbox_stream.go:48-50
func (b *inboxMemberCollection) MappingUpdate() (string, json.RawMessage) { return "", nil }
```

If `spotlight-{site}-v1` was ever auto-created by a bulk write before the template
existed, it keeps ES dynamic defaults forever: `userAccount` becomes `text` +
`.keyword`. The handler's access filter is an exact `term`:

```go
// search-service/query_rooms.go:30
map[string]any{"term": map[string]any{"userAccount": account}},
```

Against a `text` field, `term "John.Smith"` looks for that literal token while the index
holds `[john, smith]` → **zero hits for every query, for every such account**. Any
account with uppercase or a `.` breaks; a lowercase single-token account still works,
which is exactly why it can pass a smoke test and fail for real users.

Related: nothing anywhere normalizes account case between the subject token and the
indexed `userAccount`, so a case difference between producer and requester is silently
fatal even with correct `keyword` mappings.

### H6 — the same logical index has two different env var names

| Service | Env var | Source |
|---|---|---|
| search-service | `SEARCH_SPOTLIGHT_INDEX` | `envPrefix:"SEARCH_"` + `env:"SPOTLIGHT_INDEX"` (`search-service/main.go:74,90`) |
| search-sync-worker | `SPOTLIGHT_INDEX` | `search-sync-worker/main.go:47` |
| es-index-migrator | `SPOTLIGHT_INDEX` | `data-migration/es-index-migrator/config.go:19` |

Same asymmetry for `SEARCH_USER_ROOM_INDEX` vs `USER_ROOM_INDEX`. Two names for one
value is a standing invitation to drift, and thanks to §1 the drift is **silent**: reads
go to a pattern nothing writes, and the reply is `[]`.

Note the `-v<N>` guard (`search-service/main.go:110-114`) only rejects a *malformed*
value at startup — a well-formed but *wrong* value starts cleanly and returns `[]` forever.

---

## 4. Resolution sketch

Ordered so the diagnosis lands before the repairs. Every item is TDD: failing test first.

### F1 — make the failure legible (do this first, it is what makes the rest fast)

1. **Distinguish "no index" from "no hits".** In `searchRooms`, when ES returns zero
   hits, resolve the read pattern once (`GET <pattern>/_alias` or the `_search` response's
   `_shards.total == 0`) and `slog.Warn` with `pattern`, `account`, `query` when the
   pattern matches no concrete index. Cheap, only on the zero-hit path.
2. **Startup readiness check.** After config parse, assert the spotlight read pattern
   resolves to ≥1 index and log its doc count at INFO. A service that reads an index
   nobody writes should say so at boot, not on every empty reply.
3. **Metric.** Add a `search_rooms_empty_total{reason}` counter alongside the existing
   `metricKindRooms` instrumentation (`search-service/metrics.go`), with
   `reason ∈ {no_index, no_docs_for_account, no_match}`. This is the signal that would
   have answered the question without this investigation.
4. **Fail loudly on a dead writer.** search-sync-worker already `os.Exit(1)`s on consumer
   creation failure, which is correct — but the *readiness* probe should also fail while a
   collection has no running consumer, so a crash-looping writer surfaces as a red
   deployment rather than a quietly stale index.

### F2 — one env var name per logical index

Accept `SPOTLIGHT_INDEX` / `USER_ROOM_INDEX` (unprefixed) in search-service as the
canonical names, keeping `SEARCH_*` readable for one release for compatibility, and log a
deprecation warning when only the prefixed form is set. Add a startup check that the
resolved base matches what the worker writes where both are observable. This removes the
whole H6 class.

### F3 — DM rooms: one source of truth for `roomName`

The live path and the backfill must produce the same document. Two options — **B is
recommended**:

- **A.** Populate `MemberAddEvent.RoomName` for DMs at the producer. Problem: a DM's name
  is *per-viewer* (each side sees the counterpart), and `MemberAddEvent` carries one name
  for an `Accounts[]` list. It does not fit the event shape.
- **B.** Give `MemberAddEvent` a per-account name — index the DM document from the
  subscription name the way the backfill already does. Concretely: extend the payload with
  `AccountNames map[string]string` (or emit one event per account on the DM path) and have
  `newSpotlightSearchIndex` prefer the per-account name, falling back to `evt.RoomName`.
  This makes `spotlightaction.go` and `spotlight.go` structurally agree.

Then close H2: `publishSyncDMInbox` must publish an `InboxInternal` `member_added` for the
**local** participant on every DM — including the same-site case it currently returns
early on — mirroring `finishCreateRoom`'s local publish.

Tests: a DM created same-site and a DM created cross-site both appear in
`search.rooms` for both participants; a `roomType: "dm"` query returns them.

### F4 — subscribe spotlight to `room_renamed`

Add `room_renamed` to the spotlight consumer's filter subjects (a *new* subject set, not
`InboxMemberEventSubjects` — user-room must not start consuming renames), and handle it in
`spotlightCollection.BuildAction` as a partial update of `roomName` across every
`{account}_{roomID}` document for that room. An ES `_update_by_query` on
`term roomId = <id>` is the natural shape; keep the existing external-version guard so a
redelivered rename cannot clobber a newer one.

Also confirm the origin side actually emits it on both lanes — `room_renamed` currently
rides the OUTBOX/federation path, and the local internal lane needs it too.

### F5 — analyzer: cover the real mapping, then fix what that reveals

1. **Close the coverage gap first.** Change `putTestSpotlightIndex`
   (`search-service/integration_rooms_test.go:60`) to build the index from
   `searchindex.SpotlightTemplateBody(index, true)` instead of the hand-rolled mapping, so
   every room-search integration test runs against the production analyzer. Expect some to
   go red — that red *is* the bug report.
2. Add cases for a hyphenated name, a mid-word query, and a CJK room name.
3. **Then** fix the analyzer. Align spotlight with the messages template's CJK handling
   (add `cjk_bigram`, and split on `-`/`_`/`.` the way `underscore_preserving` +
   `underscore_subword` do) rather than inventing a third analyzer. Query `roomName`,
   `roomName._2gram`, `roomName._3gram` per the `search_as_you_type` convention.
4. Analyzer changes are **not** retroactive: they require a `-v2` index and a reindex. The
   wildcard read pattern already makes that rollover transparent — this is what
   `es-index-migrator` exists for.

### F6 — guarantee the mapping on an existing index

Give `spotlightCollection` a real `MappingUpdate()` returning
`(searchindex.IndexPattern(indexName), <properties>)` so startup pushes the mapping onto
already-existing indices, exactly as the messages collection does. This closes H5 for
`keyword` fields (additive mapping updates are legal); an analyzer change on an existing
field still needs the F5.4 reindex.

Add a startup assertion that `userAccount` is mapped as `keyword`, and fail fast if it is
`text` — that mapping silently breaks the *entire access filter*, which is worse than not
starting.

### Recovery lever, independent of the fix

`data-migration/es-index-migrator` rebuilds spotlight from Mongo `subscriptions`
(`runner.go:92`, `spotlightaction.go`) and is the right way to repopulate the test server
once the writer is healthy and the mapping is correct. Note it inherits H3 and H4 (it
writes into whatever mapping exists and has no rename story), so run it **after** F5/F6,
not before.

---

## 5. Suggested order

1. Run §2 on the test server — 10 minutes, and it collapses seven hypotheses to one.
2. If H7: fix the stream name ops-side, restart the worker, re-run the backfill.
3. Ship **F1** regardless of which hypothesis wins. The reason this took a code audit
   instead of a log line is the real defect.
4. **F5.1** next — flipping the test fixture to the production template converts the
   remaining analyzer questions into failing tests.
5. Then F6, F3, F4, F2 as separate PRs; each is independently shippable.
