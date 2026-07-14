# Design: PR #353 follow-ups (upload/attachments)

**Date:** 2026-06-25
**Status:** Approved design — pending implementation plan
**Branch:** `claude/pr353-followups` (from `main`)

Follow-up work for the merged PR #353 (file upload + message attachments). Five
small, independent changes; a sixth investigated and found already satisfied.

## (a) Rename upload route `POST …/upload` → `…/upload/file`

`POST /api/v1/rooms/:roomId/upload` → `POST /api/v1/rooms/:roomId/upload/file`
(parallels the existing `…/upload/images`). No handler-logic change.

- `upload-service/routes.go`: update the path.
- `upload-service/handler_test.go`: the route-registration test asserts the
  registered path — update to `/upload/file`.
- `docs/client-api.md`: rename the POST endpoint heading + `**Endpoint:**` line.

No routing conflict: `POST …/upload/file` (static) vs `POST …/upload/images`
(static) vs `GET …/file/:fileId` (different method/path).

## (b) Decode attachments on the remaining Get-message RPCs

Two message-returning history RPCs miss the decode step; the rest already call
it (`LoadHistory`, `LoadNextMessages`, `LoadSurroundingMessages`,
`GetThreadMessages`, `GetThreadParentMessages`, `ListPinnedMessages`).

- `history-service/internal/service/messages.go`
  - `GetMessageByID` (returns `*models.Message`): call `decodeMessageAttachments(c, msg)`
    before returning.
  - `GetMessagesByIDs` (returns `…{Messages: kept}`): call `setDecodedAttachments(c, kept)`
    before returning.
- No change to Edit/Delete/Pin/Unpin/React responses — they carry only IDs +
  timestamps, no message body.
- Note: `GetMessageByID` is also the gatekeeper's quoted-parent fetch. Section (g)
  **relies on** this decode — the gatekeeper projection reads the parent's decoded
  `attachments` from this reply into the snapshot — so on that path the decode is
  load-bearing, not merely uniform-for-clients. (`decodeMessageAttachments` decodes
  the quoted parent too, but the gatekeeper projects only the top-level message.)

## (c) `maxAttachments` / `maxAttachmentBytes` → env config (message-gatekeeper)

Replace the package consts in `message-gatekeeper/handler.go` with handler fields
fed from config.

- `message-gatekeeper/main.go` config: add
  `MaxAttachments int \`env:"MAX_ATTACHMENTS" envDefault:"1"\`` and
  `MaxAttachmentBytes int \`env:"MAX_ATTACHMENT_BYTES" envDefault:"8192"\``.
- `NewHandler`: append `maxAttachments, maxAttachmentBytes int` params; store on
  the `Handler`. `processMessage` reads `h.maxAttachments` / `h.maxAttachmentBytes`.
- Update `main.go`'s `NewHandler(...)` call and the test `NewHandler` call sites
  (pass `1, 8192` to preserve current behavior).
- Defaults match today's consts, so behavior is unchanged unless overridden.

## (d) Add `FileType` to `Attachment`

A single canonical MIME field on every attachment.

- `pkg/model/cassandra/attachment.go`: add
  `FileType string \`json:"fileType,omitempty"\`` to `Attachment` (which
  `model.Attachment` aliases).
- `upload-service/attachment.go` `buildAttachment`: set `att.FileType = m.mime`
  (already lowercased/param-stripped) for **all** attachment types
  (image/audio/video/generic).
- The media-specific `imageType`/`audioType`/`videoType` remain (kept for the
  existing FE; `fileType` is the one canonical field present on every attachment).
- `docs/client-api.md`: add `fileType` to the `Attachment` schema table.

## (e) `mediatype.go` — exact-match map for O(1) lookup

Keep the same allow/deny semantics; back exact entries with a map.

- `mediaTypeFilter` gains, per list, a `map[string]struct{}` of exact media types
  plus a `[]string` of wildcard patterns (`type/*`, `*`). Built once in
  `newMediaTypeFilter`.
- `allowed(mime)`: normalize, then for blacklist (deny wins) and whitelist —
  check the exact map (O(1)); if no exact hit, scan the wildcard slice. Same
  result as today; existing `mediatype_test.go` cases stay green.
- Perf gain is marginal (lists are tiny), but the structure is the requested
  cleaner form.

## (f) broadcast attachments inside Message — make the wrapper decode explicit

**Decision: keep Option 1 (the wrapper owns the decoded attachments); harden the
implicit field-shadow it relies on.** The live broadcast `ClientMessage` already
serializes decoded `attachments` (objects, not base64) and nested
`quotedParentMessage` inside the inline-flattened message, and encrypted rooms
still encrypt the whole message into `encryptedMessage`. That observable behavior
is correct and stays. What changes is **how** it is expressed: today it leans on
an undocumented shadow that nothing pins.

**Architecture (Option 1 — transport untouched).** `model.Message` stays the raw
transport/persistence type — `Attachments [][]byte`, `json:"attachments,omitempty"`
— so the canonical wire between message-gatekeeper and the workers, JetStream
redeliveries, and federation replays are unchanged. The decoded `[]Attachment`
representation lives **only** on the client-edge wrapper `ClientMessage`.
(Rejected: Option 2 — moving `DecodedAttachments json:"attachments"` + raw
`json:"-"` onto `model.Message` — which forces a gatekeeper decode + a
message-worker re-encode and a new wire-compat surface for no client-visible gain.)
Note this does not conflict with (g): (g) takes on the same decode+re-encode shape
for the **quoted parent** because there it delivers a client-visible feature (the
quoted preview gains attachments); Option 2 would impose it on the **main** message,
which the wrapper already serves decoded — so there it is pure cost.

**The shadow, named.** `ClientMessage` embeds `Message` and *also* declares
`Attachments []Attachment json:"attachments,omitempty"`. Two fields claim the
`attachments` JSON key; Go's `encoding/json` promotion rule (the shallower field
wins, the deeper embedded field is suppressed entirely) makes the wrapper's
decoded slice the one emitted and the embedded raw blobs invisible. This mirrors
the established two-field pattern on `cassandra.Message` /
`cassandra.QuotedParentMessage` (`Attachments [][]byte json:"-"` +
`DecodedAttachments []Attachment json:"attachments"`), except the wrapper cannot
re-tag the embedded field `json:"-"`, so it depends on promotion precedence
instead — and on `sonic` (broadcast-worker's marshal codec) replicating that
precedence on the hot path.

**Hardening (the actual change):**

1. `pkg/model/event.go` — document the field. Add a comment on
   `ClientMessage.Attachments` stating it is the sole client-facing attachments
   representation and deliberately shadows the embedded raw `Message.Attachments`
   via promotion precedence.
2. `broadcast-worker/handler.go` `buildClientMessage` — after `cm.Attachments =
   decoded`, set `cm.Message.Attachments = nil` so the wrapper holds exactly one
   representation in memory; the embedded raw blobs are an internal transport
   detail, never client-facing. Safe: `Message: *msg` is a value copy, so
   reassigning the copy's `Attachments` slice header does not touch the caller's
   canonical `*msg`. This does not change today's serialized bytes (the
   promotion rule already suppresses the embedded field); it removes the dual
   in-memory representation, makes intent explicit, and is defense-in-depth for any
   future change to the embedding.
3. Tests:
   - `broadcast-worker/sonic_wire_test.go` — add a fixture whose `ClientMessage`
     carries BOTH embedded raw `Message.Attachments` and a decoded wrapper
     `Attachments`, and assert that **sonic** emits `attachments` as objects
     (`"id":`), never the base64 of the raw blob, and that sonic ≡ stdlib
     (`JSONEq`). This is the real guard — it pins that sonic honors stdlib's
     shadow/promotion behavior. (Today's `TestBuildClientMessage_DecodesAttachments`
     only exercises `encoding/json`.)
   - `broadcast-worker/handler_test.go` `TestBuildClientMessage_DecodesAttachments`
     — additionally assert `cm.Message.Attachments == nil` after build. (Section (g)
     revises this same test's quoted-parent setup to arrive pre-decoded — coordinate
     the two edits.)

No `model.Message` change and no `docs/client-api.md` change (wire output is
unchanged). The reaction `NotificationEvent` still carries raw `[][]byte`
attachments, but it is a notification ping, not a rendered message — left as-is.

## (g) Propagate quoted-parent attachments through the snapshot

**Goal:** a quoted / replied-to preview should carry the parent's attachments.
Today the gatekeeper's `quotedParentProjection` omits attachments, so the snapshot
— and every consumer of it — has none. This threads the parent's attachments
end-to-end.

**The representation split that shapes the design.** On
`cassandra.QuotedParentMessage`:

```go
Attachments        [][]byte     `json:"-"           cql:"attachments"`  // raw — Cassandra column, never on the wire
DecodedAttachments []Attachment `json:"attachments" cql:"-"`            // decoded — crosses every NATS wire
```

So the parent's attachments travel as **decoded** objects (the only form any NATS
reply or the canonical event carries) and are **re-encoded to raw only at the
Cassandra boundary**. Three coordinated changes:

**1. message-gatekeeper — capture decoded attachments into the snapshot.**
- `fetcher_history.go`: add `DecodedAttachments []Attachment \`json:"attachments"\``
  to `quotedParentProjection`. history-service's `GetMessageByID` reply already
  decodes the parent's attachments under the `attachments` key (section (b) +
  `decodeMessageAttachments`), so the projection reads them directly.
- `FetchQuotedParent`: set `DecodedAttachments: parent.DecodedAttachments` on the
  returned snapshot.
- Effect: the canonical message's `quotedParentMessage` now serializes
  `attachments` (decoded) on the wire. (Redacted parents already decode to nil
  upstream, so they carry no attachments — unchanged.)

**2. broadcast-worker — drop the now-redundant decode.**
- `buildClientMessage`: remove the quoted-parent clone-and-decode block

  ```go
  if msg.QuotedParentMessage != nil {
      qp := *msg.QuotedParentMessage
      qp.DecodedAttachments, _ = cassandra.DecodeAttachments(qp.Attachments)
      cm.QuotedParentMessage = &qp
  }
  ```

  entirely. The quoted parent already arrives with `DecodedAttachments` populated
  (via the embedded `Message: *msg` copy); the old decode ran on the empty raw
  `qp.Attachments` (`json:"-"`, never on the wire) and would now **clobber** the
  good value with nil. The clone existed only to avoid mutating the caller's
  `*msg` during that decode — with no mutation it is unnecessary. (Coordinate with
  section (f): same function — (f) nils the **main** message's
  `cm.Message.Attachments`, (g) removes the **quoted-parent** decode.)

**3. message-worker — re-encode decoded → raw before the Cassandra write.**
- Add `EncodeAttachments(atts []Attachment) [][]byte` to
  `pkg/model/cassandra/attachment.go` — the inverse of `DecodeAttachments`
  (`json.Marshal` each `Attachment`).
- In `buildCassandraMessage` (`store_cassandra.go`), where it copies the quoted
  parent into a fresh struct (`q := *msg.QuotedParentMessage`), set
  `q.Attachments = cassandra.EncodeAttachments(q.DecodedAttachments)`. gocql writes
  the `attachments LIST<BLOB>` column from the raw `Attachments` field
  (`cql:"attachments"`; `DecodedAttachments` is `cql:"-"`), so without this the
  column persists empty. This must sit **before** encryption — `buildCassandraMessage`
  feeds the encrypt step and the quoted parent's `Attachments` is one of the
  encrypted fields (per the function's own doc comment). The fresh-struct copy
  already isolates the caller's `*msg`, so writing `q.Attachments` is safe.
- The **main** message needs no re-encode: its raw `msg.Attachments` rides the
  canonical wire (`model.Message.Attachments json:"attachments"`), so message-worker
  already holds the bytes.
- The re-encoded bytes are a normalized re-serialization of the `Attachment`
  struct (all modeled fields incl. `fileType` from (d) preserved); byte-identity
  with the original upload blob is neither guaranteed nor required — the gatekeeper
  only ever had the decoded form.

**Client API.** `docs/client-api.md`: the `quotedParentMessage` in message payloads
now includes `attachments` (`Attachment[]`). Update its schema reference.

**Tests.**
- gatekeeper: `FetchQuotedParent` populates the snapshot's `DecodedAttachments`
  from a reply carrying `attachments`.
- broadcast-worker: `TestBuildClientMessage_DecodesAttachments` — quoted parent
  arrives pre-decoded; assert pass-through and no dependence on raw `qp.Attachments`.
- message-worker: round-trip the quoted-parent `attachments` column (decoded-in →
  `EncodeAttachments` → stored → `DecodeAttachments` equal); plus an
  `EncodeAttachments`/`DecodeAttachments` inverse unit test.

## Testing

- (a) route-registration test asserts `/upload/file`; (b) unit tests assert
  decoded `attachments` on `GetMessageByID` / `GetMessagesByIDs`; (c) gatekeeper
  tests cover default + overridden caps; (d) `buildAttachment` tests assert
  `fileType` set per family; (e) existing `mediatype_test.go` plus a couple
  added cases (exact map hit, wildcard miss) stay green; (f) sonic wire test
  asserts decoded `attachments` objects (never base64) on a `ClientMessage` with
  raw embedded blobs, and `buildClientMessage` nils the embedded raw; (g)
  gatekeeper snapshot carries decoded quoted-parent attachments, broadcast no
  longer decodes them, message-worker round-trips the re-encoded raw column.
- Per-service: `make test SERVICE=<svc>` + `make lint`.

## Out of scope

- Full broadcast/history message-schema unification (field names) — separate work.
- Frontend changes (compose `msg.send`, render `attachments`, use `/file/` links).
