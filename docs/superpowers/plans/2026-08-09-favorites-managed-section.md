# Favorites becomes a manually move-managed section

Shape A2: `moveChat` accepts `sectionId: "favorites"` as a valid target (the one
built-in exception), so favorites gets manual ordering (`sectionOrder` /
`afterRoomId` / `beforeRoomId`) via the existing chat-move RPC instead of only the
favorite-toggle bit. Membership stays FE-derived from `Subscription.favorite`;
`sectionId`/`favorite` are kept mirrored in both write paths (moveChat,
favorite.toggle) and both directions of cross-site replication.

## 7-segment trace

1. **Client request + `docs/client-api.md`** — no new field on `MoveChatRequest`.
   Doc change only: `favorites` documented as a valid `sectionId` target for
   `chat.move`, and the favorite-flag interaction on both `favorite.toggle` and
   `chat.move`. Derived views (`docs/client-api/request-reply.md`) synced by hand.
2. **Gatekeeper** — N/A (room-service owns both RPCs directly; no gatekeeper hop
   for chat.move / favorite.toggle).
3. **Canonical model / events** — no new event type. `SubscriptionSectionMovedEvent`
   (unchanged shape: `account`, `roomId`, `sectionId`, `sectionOrder`, `timestamp`)
   is reused; the remote inbox-worker derives the favorite mirror from
   `sectionId == "favorites"` itself rather than the event carrying a new field.
   Same for `SubscriptionFavoriteToggledEvent` (unchanged) — the remote mirror now
   also clears `sectionId` server-side when favorite goes false and the stored
   `sectionId` is `"favorites"`.
4. **Workers** — `inbox-worker.handleSubscriptionSectionMoved` (mirrors favorite
   on/off) and `handleSubscriptionFavoriteToggled` (mirrors the section clear) —
   both change; no new handler.
5. **Storage** — Mongo `subscriptions` collection:
   - `room-service` `MoveSubscriptionSection` (origin write) now also sets
     `favorite` (true iff target is `"favorites"`, false otherwise incl. remove).
   - `room-service` `ToggleSubscriptionFavorite` (origin write) now also clears
     `sectionId`/`sectionOrder` via an aggregation-pipeline `$cond` when turning
     favorite off and the stored section was `"favorites"`.
   - `inbox-worker` `mongoInboxStore.UpdateSubscriptionSection` and
     `UpdateSubscriptionFavorite` mirror both of the above on the remote replica.
   - No new index — the existing `{u.account, sectionId, sectionOrder}` section
     index already covers `sectionId == "favorites"` rows.
6. **Read/history path** — N/A (chatlist read model is unchanged; favorites
   membership is still derived client-side from `favorite`, ordering from
   `sectionOrder` when the section's `sortMode == "custom"`, same as any other
   section).
7. **Client-facing events** — `subscription.update` (`action: "section_moved"` /
   `"favorite_toggled"`) already fans out the full post-write `Subscription`
   document, so `favorite` + `sectionId` land together in one event with no
   extra publish.

## Gate: moveChat built-in check

`room-service/handler.go` `moveChat` rejected every `model.IsBuiltinSection` id.
Now: `model.IsBuiltinSection(*req.SectionID) && *req.SectionID != model.SectionFavorites`
— favorites is the one built-in that's a valid manual target; apps/teams/chats stay
rejected (their membership has no manual axis).

## favorite.toggle ⇄ chat.move reconciliation

- Move **into** favorites (`chat.move` with `sectionId: "favorites"`) → sets
  `favorite: true` **and** `sectionId: "favorites"` + a normal `sectionOrder`
  (append/insert via the existing `ComputeSectionOrder`/`RebalanceSection` path —
  favorites is just another section id for ordering purposes).
- Move **out** of favorites (`chat.move` to any other section, or a remove /
  `sectionId: null`) → sets `favorite: false`.
- **Toggle ON** (`favorite.toggle`) → leaves `sectionId` untouched. Membership is
  already satisfied by the flag; a bare toggle doesn't imply a manual order, so it
  doesn't force the chat into the ordered set. Un-ambiguous: the flag alone is
  sufficient for FE membership rendering (per acceptance).
- **Toggle OFF** (`favorite.toggle`) → also clears `sectionId`/`sectionOrder` when
  they were `"favorites"`. Rationale: `favorite.toggle` off is meant to "unfavorite
  and remove it from the section" — leaving a stale `sectionId: "favorites"` on a
  now-unfavorited row would let the manual section resurrect the chat under
  favorites without a value that says why, and would violate "favorite==true ⇔
  sectionId=='favorites'" (the doc'd invariant this PR maintains). Implemented
  atomically via an aggregation-pipeline `$set`/`$cond` in `ToggleSubscriptionFavorite`
  (origin) and mirrored on the remote replica in `UpdateSubscriptionFavorite`
  (inbox-worker) so both sites converge to the same shape without a race window.
  This closes what would otherwise be a real cross-site divergence gap rather than
  leaving it as a documented ceiling — the extra `$cond` clause is a handful of
  lines and reuses the section index already in place, so there's no reason to skip
  it here.
- **Rejected alternative:** keep `sectionId` set after an unfavorite (i.e. only the
  flag governs membership, `sectionId` is "sticky"). Rejected because it lets a
  re-favorite later silently resurrect the OLD manual position instead of a fresh
  append, which is surprising and undocumented — and it would mean `sectionId`
  can point at `"favorites"` while `favorite == false`, breaking the invariant the
  acceptance criteria states explicitly ("kept in sync").

## Cross-site consistency

Both `chat.move`→favorites and `favorite.toggle`→off reconcile `sectionId`/
`favorite` **on the origin write**. The existing federation events
(`SubscriptionSectionMovedEvent`, `SubscriptionFavoriteToggledEvent`) already carry
enough information (`sectionId` for the move event; the toggle event's `Account`/
`RoomID` for the favorite event, which the inbox-worker resolves against the
*locally stored* `sectionId`) for the remote inbox-worker to reconstruct the same
mirror without a new event field — see segment 3/4/5 above.
