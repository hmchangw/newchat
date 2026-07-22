# Room-list preview enrichment (`PreviewMessage`) — Design

**Issue:** #110. Builds on #103/#104 (skip system + quoted messages in the preview walk).

## Problem

The room-list preview (`rooms.get` → `SubscriptionRoom.PreviewMessage`) carried only
`{messageId, sender, content, createdAt}`, and `sender` was the raw `cassandra.Participant`
(`id`/`account` only). The frontend can't render an attachment icon, mentioned names, or a
bot's app name from that. Every needed source field is already read by the walk's
projection (`messages_by_room.go` `baseColumns` selects `mentions`, `attachments`,
`visible_to`) — this is a mapping + sender-enrichment change, not a storage change.

## Approach

Replace the minimal `LastMessage` wire type with a dedicated **`PreviewMessage`**
(`pkg/model/message.go`) and map it in the existing `roomLastMessage` walk via a new
`toPreviewMessage` mapper.

- **Sender / mentions → wire `Participant`.** Reuse `toWireParticipant` (`chineseName` from
  the Cassandra `company_name`). The sender's `displayName` is composed, and for a bot
  account it's the app name — this "compose, then bot-override" logic is **extracted from
  the reaction path** into a shared `HistoryService.botAwareDisplayName` helper (previously
  inline in `ReactMessage`), so both callers stay in sync.
- **Attachments.** The walk reads raw `attachments` blobs but — unlike every other read
  path — never called `setDecodedAttachments`. `toPreviewMessage` decodes the single
  eligible message via the existing `decodeMessageAttachments` before mapping.
- **`visibleTo`.** Surfaced from the already-projected column. Its **write-path is a
  separate follow-up** (no writers today), so the field is empty until that lands.
- **`forwardSource`.** Deferred — depends on #106 (`Forwarded` snapshot). Only a
  `// TODO(#106)` marker is left.

## Scope

- No Cassandra/Mongo schema or projection change (columns already projected).
- The `LastMessage` → `PreviewMessage` rename ripples through the shared wire type and its
  consumers (history models alias, `user-service` `RoomsGet` signature + `SubscriptionRoom`
  field, historyclient, mocks). The `SubscriptionRoom.PreviewMessage` **field** (renamed from `LastMessage` per review);
  only its type changes.

## Out of scope

- `visibleTo` write-path (separate BE task).
- `forwardSource` wiring (after #106).
- Thread-list preview.
