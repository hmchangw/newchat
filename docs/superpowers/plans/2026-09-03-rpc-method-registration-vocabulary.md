# RPC Method at Registration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `rpc_method` a required argument of every reply-registering `natsrouter` route so each of the fleet's 92 RPC routes carries its own verb-first name, and stop recording fire-and-forget routes in the RPC histogram.

**Architecture:** `rpc_method` stops being inferred from the NATS subject at dispatch time and becomes a value supplied at route registration. `natsrouter.Register`, `RegisterNoBody` and `RegisterOptionalBody` gain a required `natsmetrics.RPCMethod` parameter; `RegisterVoid` deliberately has none and records no RPC sample. The shared `Operation` enum splits into `PublishOperation` (publish-failure family) and `RPCMethod` (both RPC families), so the two no longer multiply into each other's label space. `RequestOperationFromSubject` and the hand-maintained route table are deleted.

**Tech Stack:** Go 1.25, `pkg/natsrouter`, `pkg/natsmetrics`, OpenTelemetry RPC semconv, `go.uber.org/mock`, `stretchr/testify`.

**Spec:** This document is self-contained; it supersedes the vocabulary landed in commit `2f5bfe4` on this branch. Background lives in `docs/specs/o11y/nats-metrics-contract.md` §13.1 and `docs/load-testing/common/sli-slo.md` §3, §8.

## Global Constraints

- Go 1.25. Single `go.mod` at repo root. No new third-party dependencies.
- Every label value comes from a closed, code-owned enum. Client-supplied values must never become a metric label (`docs/specs/o11y/nats-metrics-contract.md` §2, enforced by `.semgrep/metrics.yml`'s `metrics-no-unbounded-label`).
- **RPC method naming: `<verb>_<object>[_qualifier]`, lower `snake_case`, verb first.** `rename_room` not `room_rename`; `get_message` not `message_get`. `service_name` already carries the domain, so no noun prefix for grouping. Matches Google AIP-136 and the Protobuf style guide; OpenTelemetry only requires `rpc.method` be a stable, identifiable logical method.
- No dynamic value (account, room id, site id, message id) may appear in a method name.
- Commands run through `make`, never raw `go`. `SERVICE=` is spliced into `./$(SERVICE)/...` (`Makefile:108`), so a package under `pkg/` needs its full path: `make test SERVICE=pkg/natsmetrics`, not `SERVICE=natsmetrics`. There is no whole-repo build target. `make lint` runs `golangci-lint run ./...` and fails on a compile error, but **it is not a complete compile gate**: `.golangci.yml` sets no `build-tags`, so files behind `//go:build integration` are never typechecked by it or by `make test`. Those files are listed explicitly in Task 2 and verified with `make test-integration SERVICE=<name>`, which needs Docker; where Docker is unavailable locally, CI owns that leg.
- TDD is mandatory: write the failing test, watch it fail, then implement.
- All tests run with `-race` (the Makefile handles this).
- `make sast` is a blocking CI gate. In this container `govulncheck` and the remote semgrep registry are blocked by proxy policy (403); run `make sast-gosec` and `make sast-semgrep-test` locally and leave the other two to CI.

---

## Naming Table

This is the complete vocabulary. Names are derived from each route's handler
function, which is the repo's own statement of what the route does.

### room-service (27)

| Subject builder | Handler | `rpc_method` |
|---|---|---|
| `MuteTogglePattern` | `h.muteToggle` | `toggle_mute` |
| `FavoriteTogglePattern` | `h.favoriteToggle` | `toggle_favorite` |
| `MoveChatPattern` | `h.moveChat` | `move_chat` |
| `OpenRoomPattern` | `h.openRoom` | `open_room` |
| `RoomAppTabsPattern` | `h.getRoomAppTabs` | `get_room_app_tabs` |
| `RoomAppCmdMenuPattern` | `h.getRoomAppCommandMenu` | `get_room_app_command_menu` |
| `OrgMembersPattern` | `h.listOrgMembers` | `list_org_members` |
| `MemberListPattern` | `h.listMembers` | `list_members` |
| `MemberStatusesPattern` | `h.listMemberStatuses` | `list_member_statuses` |
| `MentionableSubscriptionsPattern` | `h.listMentionableSubscriptions` | `list_mentionable_subscriptions` |
| `RoomKeyGetPattern` | `h.getRoomKey` | `get_room_key` |
| `MessageReadPattern` | `h.messageRead` | `mark_message_read` |
| `MessageReadReceiptPattern` | `h.messageReadReceipt` | `list_message_readers` |
| `MessageThreadReadPattern` | `h.messageThreadRead` | `mark_thread_read` |
| `MemberRoleUpdatePattern` | `h.updateRole` | `update_member_role` |
| `MemberRemovePattern` | `h.removeMember` | `remove_member` |
| `MemberAddPattern` | `h.addMembers` | `add_members` |
| `RoomRenamePattern` | `h.roomRename` | `rename_room` |
| `RoomRestricted` | `h.roomRestricted` | `set_room_restricted` |
| `RoomsInfoBatchSubscribe` | `h.roomsInfoBatch` | `batch_get_rooms_info` |
| `ThreadRoomInfoBatch` | `h.threadRoomInfoBatch` | `batch_get_thread_rooms_info` |
| `RoomThreadReadAllSubscribe` | `h.clearAllThreadRead` | `mark_all_threads_read` |
| `RoomKeyEnsure` | `h.ensureRoomKey` | `ensure_room_key` |
| `RoomCreatePattern` | `h.createRoom` | `create_room` |
| `TeamsRoomCallPattern` | `h.teamsRoomCall` | `start_teams_room_call` |
| `TeamsUserCallPattern` | `h.teamsUserCall` | `start_teams_user_call` |
| `TeamsMeetingPattern` | `h.teamsMeeting` | `create_teams_meeting` |

### history-service (17)

| Subject builder | Handler | `rpc_method` |
|---|---|---|
| `MsgHistoryPattern` | `s.LoadHistory` | `get_channel_history` **(SLO-4)** |
| `MsgThreadPattern` | `s.GetThreadMessages` | `get_thread_messages` **(SLO-5)** |
| `MsgNextPattern` | `s.LoadNextMessages` | `get_next_messages` |
| `MsgSurroundingPattern` | `s.LoadSurroundingMessages` | `get_surrounding_messages` |
| `MsgGetPattern` | `s.GetMessageByID` | `get_message` |
| `MsgGetIDsPattern` | `s.GetMessagesByIDs` | `batch_get_messages` |
| `RoomsGet` | `s.RoomsGet` | `batch_get_rooms` |
| `MsgPinnedListPattern` | `s.ListPinnedMessages` | `list_pinned_messages` |
| `MsgThreadParentPattern` | `s.GetThreadParentMessages` | `get_thread_parent_messages` |
| `ThreadSubscriptionList` | `s.ListThreadSubscriptions` | `list_thread_subscriptions` |
| `MsgEditPattern` | `s.EditMessage` | `edit_message` |
| `MsgDeletePattern` | `s.DeleteMessage` | `delete_message` |
| `MsgPinPattern` | `s.PinMessage` | `pin_message` |
| `MsgUnpinPattern` | `s.UnpinMessage` | `unpin_message` |
| `MsgReactPattern` | `s.ReactMessage` | `toggle_message_reaction` |
| `MigrationInternalMsgEdit` | `s.MigrationEditMessage` | `migrate_edit_message` |
| `MigrationInternalMsgDelete` | `s.MigrationDeleteMessage` | `migrate_delete_message` |

### user-service (29)

| Subject builder | Handler | `rpc_method` |
|---|---|---|
| `UserMePattern` | `s.Me` | `get_current_user` |
| `UserStatusGetByNamePattern` | `s.GetStatusByName` | `get_user_status` |
| `UserProfileGetByNamePattern` | `s.GetProfileByName` | `get_user_profile` |
| `UserStatusSetPattern` | `s.SetStatus` | `set_user_status` |
| `UserSettingsGetPattern` | `s.GetSettings` | `get_settings` |
| `UserSettingsSetPattern` | `s.SetSettings` | `set_settings` |
| `UserPriorityContactsGetPattern` | `s.GetPriorityContacts` | `list_priority_contacts` |
| `UserPriorityContactsAddPattern` | `s.AddPriorityContact` | `add_priority_contact` |
| `UserPriorityContactsRemovePattern` | `s.RemovePriorityContact` | `remove_priority_contact` |
| `UserChatlistGetPattern` | `s.GetChatlist` | `get_chatlist` |
| `UserChatlistSectionCreatePattern` | `s.CreateChatlistSection` | `create_chatlist_section` |
| `UserChatlistSectionDeletePattern` | `s.DeleteChatlistSection` | `delete_chatlist_section` |
| `UserChatlistSectionRenamePattern` | `s.RenameChatlistSection` | `rename_chatlist_section` |
| `UserChatlistSectionReorderPattern` | `s.ReorderChatlistSections` | `reorder_chatlist_sections` |
| `UserChatlistSectionSetSortModePattern` | `s.SetChatlistSectionSortMode` | `set_chatlist_section_sort_mode` |
| `UserSubscriptionListPattern` | `s.ListSubscriptions` | `list_subscriptions` |
| `UserThreadListPattern` | `s.ListUserThreads` | `list_user_threads` |
| `UserThreadUnreadSummaryPattern` | `s.GetThreadUnreadSummary` | `get_thread_unread_summary` |
| `UserThreadReadAllPattern` | `s.ClearAllThreadUnread` | `mark_all_threads_read` † |
| `UserSubscriptionGetChannelsPattern` | `s.GetChannels` | `list_channel_subscriptions` |
| `UserSubscriptionGetDMPattern` | `s.GetDM` | `get_dm_subscription` |
| `UserSubscriptionGetByRoomIDPattern` | `s.GetByRoomID` | `get_subscription_by_room` |
| `UserSubscriptionCountPattern` | `s.CountSubscriptions` | `count_subscriptions` |
| `UserSubscriptionSetAppSubscriptionPattern` | `s.SetAppSubscription` | `set_app_subscription` |
| `UserAppsListPattern` | `s.ListApps` | `list_apps` |
| `UserAppsCategoriesPattern` | `s.ListAppCategories` | `list_app_categories` |
| `UserSSOSetPattern` | `s.SSOSet` | `set_sso_token` |
| `UserSSORefreshPattern` | `s.SSORefresh` | `refresh_sso_token` |
| `BadgeCountBatchPattern` | `s.BadgeCountBatch` | `batch_get_badge_counts` |

### search-service (5), media-service (2), translation-service (1), room-worker (1)

| Service | Subject builder | Handler | `rpc_method` |
|---|---|---|---|
| search-service | `SearchMessagesPattern` | `h.searchMessages` | `search_messages` |
| search-service | `SearchRoomsPattern` | `h.searchRooms` | `search_rooms` |
| search-service | `SearchAppsPattern` | `h.searchApps` | `search_apps` |
| search-service | `SearchUsersPattern` | `h.searchUsers` | `search_users` |
| search-service | `SearchOrgsPattern` | `h.searchOrgs` | `search_orgs` |
| media-service | `EmojiListPattern` | `h.HandleEmojiList` | `list_emojis` |
| media-service | `EmojiDeletePattern` | `h.HandleEmojiDelete` | `delete_emoji` |
| translation-service | `TranslateRequestPattern` | `handler.Translate` | `translate_text` |
| room-worker | `RoomCreateDMSync` | `handler.serverCreateDM` | `create_dm_room` |

### user-presence-service (3 recorded, 4 deliberately not)

| Subject builder | Handler | `rpc_method` |
|---|---|---|
| `PresenceManualSetPattern` | `handler.SetManual` | `set_manual_presence` |
| `PresenceQueryBatch` | `handler.QueryBatch` | `batch_get_presence` |
| `PresenceQueryBatchPeer` | `handler.QueryBatchPeer` | `batch_get_peer_presence` |
| `PresenceHelloPattern` | `handler.Hello` | *(RegisterVoid — no method, no sample)* |
| `PresencePingPattern` | `handler.Ping` | *(RegisterVoid — no method, no sample)* |
| `PresenceActivityPattern` | `handler.Activity` | *(RegisterVoid — no method, no sample)* |
| `PresenceByePattern` | `handler.Bye` | *(RegisterVoid — no method, no sample)* |

† `mark_all_threads_read` is registered by **two** services: room-service's
`chat.server.request.room.{site}.thread.read.all` and user-service's
`chat.user.{account}.request.user.{site}.thread.read.all`, which delegates to
it through `user-service/roomclient`. They are the two halves of one logical
operation, so they share a method for the same reason `get_message` is shared
between history-service and its four callers. **The identity key is
`service_name` + `rpc_method`, so a method must be unique within a router, not
across the fleet** — Task 2 enforces exactly that.

### bot lane (7)

| Service | Subject builder | Handler | `rpc_method` |
|---|---|---|---|
| bot-room-service | `ServerBotRoomCreate` | `h.handleCreate` | `create_bot_room` |
| bot-room-service | `ServerBotRoomMemberAddPattern` | `h.handleAdd` | `add_bot_room_members` |
| bot-room-service | `ServerBotRoomMemberRemovePattern` | `h.handleRemove` | `remove_bot_room_members` |
| bot-room-service | `ServerBotRoomGet` | `h.handleGet` | `get_bot_room` |
| bot-room-service | `ServerBotRoomDMEnsure` | `h.handleDMEnsure` | `ensure_bot_dm_room` |
| bot-message-handler | `ServerBotMsgRoomSendPattern` | `h.handleSendRoom` | `send_room_message` |
| bot-message-handler | `ServerBotDMSendPattern` | `h.handleSendDM` | `send_dm` |

### Client-side methods (`rpc.client.call.duration`)

Six existing `Publisher.Request` call sites move onto the **same** constants
the server registers, so both halves of one call carry one `rpc.method`.

| Call site | Old constant | New constant |
|---|---|---|
| `room-service/reader_history.go:70` | `OperationHistoryGetMessage` | `MethodGetMessage` |
| `message-gatekeeper/fetcher_history.go:83` | `OperationHistoryGetMessage` | `MethodGetMessage` |
| `broadcast-worker/parent_fetcher.go:62` | `OperationHistoryGetMessage` | `MethodGetMessage` |
| `notification-worker/parent_fetcher.go:76` | `OperationHistoryGetMessage` | `MethodGetMessage` |
| `room-service/memberlist_client.go:96` | `OperationMemberRead` | `MethodListMembers` |
| `notification-worker/presence.go:81` | `OperationPresenceLookup` | `MethodGetPresenceSnapshot` |

`MethodGetPresenceSnapshot` is client-only: `chat.presence.{siteID}.request.snapshot`
has **no subscriber anywhere in the repo** and is gated off by
`PRESENCE_RPC_ENABLED=false`. Keep the constant, and record the orphan in the
contract rather than silently dropping it.

### Publish-only operations (stay `PublishOperation`)

`OperationCanonicalPublish`, `OperationRecipientPublish`,
`OperationNotificationPublish`, `OperationPushPublish`, `OperationThreadTCount`,
`OperationTeamsUserUpsert`, `OperationClientResponse`, `OperationRoomPublish`,
`OperationMemberPublish`, `OperationOutboxPublish`, `OperationUnknown`.

`OperationHistoryRead`, `OperationHistoryMutation`, `OperationRoomRead`,
`OperationRoomMutation`, `OperationMemberRead`, `OperationMemberMutation`,
`OperationTeamsRoom`, `OperationChannelHistory`, `OperationThreadOpen`,
`OperationHistoryGetMessage`, `OperationPresenceLookup`, and every constant
added in commit `2f5bfe4` are **deleted** — they were RPC labels living in the
publish enum.

---

## Rejected: `_OTHER` as the fallback value

The fallback stays spelled `"unknown"`. OpenTelemetry does **not** require
`_OTHER` for `rpc.method`: in `go.opentelemetry.io/otel@v1.44.0/semconv/v1.40.0/rpcconv`
— the package `pkg/natsmetrics/rpcsemconv.go` transcribes — `_OTHER` appears
exactly once, as `ErrorTypeOther ErrorTypeAttr = "_OTHER"`, which is the
**`error.type`** convention. `rpc.method` is defined there as free text ("the
fully-qualified logical name of the method from the RPC interface
perspective"), with no enumerated set and no sentinel. The `_OTHER` rule
belongs to `http.request.method` and `error.type`, not here.

Renaming would change an existing label value that already appears in the
contract and in queries, to conform to a convention that does not apply. After
Task 2, no registered route can produce the fallback at all, so its spelling is
close to unobservable either way.

---

## Label Cutover

Every `rpc_method` value changes at this cutover — there is no incremental
path, because the old values were resource families and the new ones are
routes. Two things follow.

**No SLO bridging is required.** SLO-4 and SLO-5 filter on
`rpc_method="channel_history"` and `rpc_method="thread_open"`
(`docs/load-testing/common/sli-slo.md:347-353`), but the SLI/SLO programme is
not live: no recording rules are running and no error budget is in flight. So
the rename costs nothing here, and there is no reason to hold those two names
back as exceptions to `<verb>_<object>`. They become `get_channel_history` and
`get_thread_messages` along with everything else — two odd names left behind to
protect a window nobody is measuring would be the worse outcome. Task 5 still
updates the queries so the document is correct on the day the SLOs do launch.

**Grafana panels do break.** Any panel filtering on `room_mutation`,
`room_read`, `member_read`, `member_mutation`, `teams_room`, `history_read`,
`history_mutation`, `channel_history` or `thread_open` stops matching. This is
a dashboard-owner pass, not a blocker: it can happen after the code ships, and
the panels that hurt most today (a per-API view of user-service) currently show
one `unknown` bar and get strictly better. The `rpc_method="unknown"` series
disappears entirely rather than shrinking, so a panel that filters it out
should have that filter removed rather than left dangling.

---

## File Structure

**Create**
- `pkg/natsmetrics/rpcmethod.go` — the `RPCMethod` type, 92 constants (91 route methods, one of them shared by two services, plus one client-only method) grouped by owning service, `normalizeRPCMethod`, and the exported `RPCMethod.Valid`.
- `pkg/natsmetrics/rpcmethod_test.go` — vocabulary guards: format, verb-first, uniqueness, length, and that the normalizer accepts every declared value.

**Modify**
- `pkg/natsmetrics/metrics.go` — rename `Operation` to `PublishOperation`; delete the RPC constants; retype `Publisher.Request` and `Publisher.HandledRequest` to take `RPCMethod`; split `requestKey`/`handledRequestKey`.
- `pkg/natsmetrics/subject.go` — delete `RequestOperationFromSubject`, `userOperation`, `searchOperation`, `emojiOperation`, `botOperation`, `isBotRequest`, `isPresenceQuery`, `isRoomFamilyRequest`, `isRequestFamily`, `isMigrationFamily`. Keep `EventTypeFromSubject`, `RoomEventTypeFromSubject`, `PublishLabelsFromSubject`, `hasAnySuffix`, `isMemberEventAt`.
- `pkg/natsmetrics/subject_test.go` — delete the `RequestOperationFromSubject` tests.
- `pkg/natsmetrics/enums_test.go` — rename `allOperations` to `allPublishOperations`, retype the `publishPair.operation` field, update `TestNormalizersAcceptEveryDeclaredValue` and `TestPublishLabelPairsAreFarNarrowerThanTheCrossProduct`, and update the budget asserts.
- `pkg/natsmetrics/rpcsemconv.go` — `rpcMethod()` takes `RPCMethod`.
- `pkg/natsmetrics/rpcsemconv_test.go`, `toggle_test.go`, `metrics_test.go`, `prometheus_export_test.go` — every use of a deleted RPC constant. `metrics_test.go:255-256` builds two dynamic values; the one passed to `Request` becomes an `RPCMethod`.
- `room-service/memberlist_client.go:38-40` — the `requestRecorder` interface takes `natsmetrics.RPCMethod`, **not** `PublishOperation`: it is the client RPC path.
- `history-service/internal/publisher/publisher.go:18` — its `Failure` interface takes `natsmetrics.PublishOperation`.
- `room-service/mock_memberlist_client_test.go` and `history-service/internal/publisher/mock_publisher_test.go` — regenerated by `make generate`, never hand-edited.
- `pkg/natsrouter/params.go:51-55` — add `method` and `recordRPC` to `route`.
- `pkg/natsrouter/router_test.go` (19 calls), `example_test.go` (6), `middleware_test.go` (4), `shutdown_test.go` (5), `integration_test.go` (7, build tag `integration`), `oversize_integration_test.go` (2, same tag) — every `Register*` call needs the new argument, and `router_test.go:114` asserts `rpc.method == "member_read"` for the `member.list` route, which becomes `list_members`.
- `room-service/integration_test.go` (7 calls) and `translation-service/integration_test.go` (1), both behind the `integration` build tag.
- `pkg/natsrouter/router.go:200-250` — `addRoute` takes a method; `natsHandler` records `rt.method` and skips recording when it is empty.
- `pkg/natsrouter/register.go` — add the method parameter to `Register`, `RegisterNoBody`, `RegisterOptionalBody`; `RegisterVoid` passes the empty method.
- The 10 registration files (96 call sites): `room-service/handler.go`, `history-service/internal/service/service.go`, `user-service/service/service.go`, `search-service/handler.go`, `media-service/emoji_nats.go`, `translation-service/main.go`, `user-presence-service/main.go`, `bot-room-service/handler.go`, `bot-message-handler/handler.go`, `room-worker/main.go`.
- The 6 client call sites listed above.
- `docs/specs/o11y/nats-metrics-contract.md`, `docs/load-testing/common/sli-slo.md`, `docs/specs/o11y/o11y-metrics-inventory.md:196-200`, `docs/load-testing/loadgen/observation.md:156`.

The publish constants keep their names, so publish call sites that merely *pass*
a constant need no edit — only the two interface declarations named above
mention the type. `pkg/natsmetrics/opttable.go` contains no `Operation`
reference and does not change.

**Delete**
- `pkg/natsmetrics/routecoverage_test.go` — the hand-maintained table. A missing method is now a compile error, so the table has nothing left to guard.

---

### Task 1: RPCMethod type and vocabulary

**Files:**
- Create: `pkg/natsmetrics/rpcmethod.go`
- Test: `pkg/natsmetrics/rpcmethod_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `type RPCMethod string`; 92 exported constants named `Method<PascalCase>` (e.g. `MethodRenameRoom`, `MethodGetMessage`); `func (m RPCMethod) Valid() bool` — **exported, because `natsrouter` must reject an unregistered method at registration**; `func normalizeRPCMethod(m RPCMethod) RPCMethod`; `const MethodUnknown RPCMethod = "unknown"`; `var allRPCMethods []RPCMethod` (test-only, declared in `rpcmethod_test.go`).

Additive only — nothing else in the repo changes in this task, so the build
stays green throughout.

- [ ] **Step 1: Write the failing vocabulary guard test**

Create `pkg/natsmetrics/rpcmethod_test.go`:

```go
package natsmetrics

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// allRPCMethods is every method the fleet registers, plus the client-only one.
// A new RPC route adds its constant here; the guards below then hold it to the
// naming rule, and Valid() rejects anything absent from it at registration.
//
// 92 entries for 92 routes plus one client-only method, because room-service
// and user-service share MethodMarkAllThreadsRead — two halves of one logical
// operation across a hop. Uniqueness that matters is per router, not per fleet;
// natsrouter enforces that.
var allRPCMethods = []RPCMethod{
	// room-service
	MethodToggleMute, MethodToggleFavorite, MethodMoveChat, MethodOpenRoom,
	MethodGetRoomAppTabs, MethodGetRoomAppCommandMenu, MethodListOrgMembers,
	MethodListMembers, MethodListMemberStatuses, MethodListMentionableSubscriptions,
	MethodGetRoomKey, MethodMarkMessageRead, MethodListMessageReaders,
	MethodMarkThreadRead, MethodUpdateMemberRole, MethodRemoveMember, MethodAddMembers,
	MethodRenameRoom, MethodSetRoomRestricted, MethodBatchGetRoomsInfo,
	MethodBatchGetThreadRoomsInfo, MethodMarkAllThreadsRead, MethodEnsureRoomKey,
	MethodCreateRoom, MethodStartTeamsRoomCall, MethodStartTeamsUserCall,
	MethodCreateTeamsMeeting,
	// history-service
	MethodGetChannelHistory, MethodGetThreadMessages, MethodGetNextMessages,
	MethodGetSurroundingMessages, MethodGetMessage, MethodBatchGetMessages,
	MethodBatchGetRooms, MethodListPinnedMessages, MethodGetThreadParentMessages,
	MethodListThreadSubscriptions, MethodEditMessage, MethodDeleteMessage,
	MethodPinMessage, MethodUnpinMessage, MethodToggleMessageReaction,
	MethodMigrateEditMessage, MethodMigrateDeleteMessage,
	// user-service (MethodMarkAllThreadsRead is shared with room-service)
	MethodGetCurrentUser, MethodGetUserStatus, MethodGetUserProfile, MethodSetUserStatus,
	MethodGetSettings, MethodSetSettings, MethodListPriorityContacts,
	MethodAddPriorityContact, MethodRemovePriorityContact, MethodGetChatlist,
	MethodCreateChatlistSection, MethodDeleteChatlistSection, MethodRenameChatlistSection,
	MethodReorderChatlistSections, MethodSetChatlistSectionSortMode,
	MethodListSubscriptions, MethodListUserThreads, MethodGetThreadUnreadSummary,
	MethodListChannelSubscriptions, MethodGetDMSubscription,
	MethodGetSubscriptionByRoom, MethodCountSubscriptions, MethodSetAppSubscription,
	MethodListApps, MethodListAppCategories, MethodSetSSOToken, MethodRefreshSSOToken,
	MethodBatchGetBadgeCounts,
	// search-service
	MethodSearchMessages, MethodSearchRooms, MethodSearchApps, MethodSearchUsers,
	MethodSearchOrgs,
	// media-service / translation-service / room-worker
	MethodListEmojis, MethodDeleteEmoji, MethodTranslateText, MethodCreateDMRoom,
	// user-presence-service
	MethodSetManualPresence, MethodBatchGetPresence, MethodBatchGetPeerPresence,
	// bot lane
	MethodCreateBotRoom, MethodAddBotRoomMembers, MethodRemoveBotRoomMembers,
	MethodGetBotRoom, MethodEnsureBotDMRoom, MethodSendRoomMessage, MethodSendDM,
	// client-only: chat.presence.{site}.request.snapshot has no subscriber in
	// this repo and is gated off by PRESENCE_RPC_ENABLED=false.
	MethodGetPresenceSnapshot,
}

var methodFormat = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)

// verbs is the closed set a method name may start with. It exists so a name
// like "room_rename" cannot land: the noun-first spelling makes every method in
// a domain sort together and read as a category, which is the shape this
// vocabulary replaced. service_name already carries the domain.
var verbs = map[string]bool{
	"add": true, "batch": true, "count": true, "create": true, "delete": true,
	"edit": true, "ensure": true, "get": true, "list": true, "mark": true,
	"migrate": true, "move": true, "open": true, "pin": true, "refresh": true,
	"remove": true, "rename": true, "reorder": true, "search": true, "send": true,
	"set": true, "start": true, "toggle": true, "translate": true, "unpin": true,
	"update": true,
}

func TestRPCMethodNamesFollowTheVocabularyRule(t *testing.T) {
	for _, m := range allRPCMethods {
		t.Run(string(m), func(t *testing.T) {
			name := string(m)
			assert.Regexp(t, methodFormat, name, "must be lower snake_case")
			assert.LessOrEqual(t, len(name), 40, "keep method names short enough to read on an axis")
			verb, _, ok := cutFirstToken(name)
			require.True(t, ok, "must be <verb>_<object>, at least two tokens")
			assert.True(t, verbs[verb], "%q is not in the allowed verb set; verb comes first", verb)
		})
	}
}

func cutFirstToken(name string) (head, tail string, ok bool) {
	for i := 0; i < len(name); i++ {
		if name[i] == '_' {
			return name[:i], name[i+1:], true
		}
	}
	return name, "", false
}

// A repeated entry in this list would mean two constants resolve to one value
// with nothing to tell them apart. It is not the per-route uniqueness guarantee
// — that one lives in natsrouter, because only a router knows which methods a
// single service registered.
func TestRPCMethodListHasNoDuplicateEntries(t *testing.T) {
	seen := map[RPCMethod]bool{}
	for _, m := range allRPCMethods {
		assert.False(t, seen[m], "duplicate rpc method %q", m)
		seen[m] = true
	}
	assert.Len(t, allRPCMethods, 92)
}

// Valid is what natsrouter calls at registration, so a gap here is a route that
// panics at startup despite naming a real constant.
func TestValidAcceptsEveryDeclaredMethodAndNothingElse(t *testing.T) {
	for _, m := range allRPCMethods {
		assert.True(t, m.Valid(), "%q must be registerable", m)
		assert.Equal(t, m, normalizeRPCMethod(m))
	}
	assert.False(t, RPCMethod("").Valid(), "empty is not a method")
	assert.False(t, RPCMethod("not_registered").Valid())
	assert.False(t, MethodUnknown.Valid(), "the fallback is not registerable")
	assert.Equal(t, MethodUnknown, normalizeRPCMethod(RPCMethod("not_registered")))
	assert.Equal(t, MethodUnknown, normalizeRPCMethod(RPCMethod("")))
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=pkg/natsmetrics`
Expected: compile failure, `undefined: RPCMethod`, `undefined: MethodToggleMute`, …

- [ ] **Step 3: Write `pkg/natsmetrics/rpcmethod.go`**

Declare `type RPCMethod string`, then one constant per row of the naming table,
grouped by owning service with a comment naming the service. **The constant name
is `Method` + the value in PascalCase**, so `rename_room` becomes
`MethodRenameRoom`, `batch_get_rooms_info` becomes `MethodBatchGetRoomsInfo`,
`set_sso_token` becomes `MethodSetSSOToken` (initialisms stay upper: SSO, DM).
The complete set is the `allRPCMethods` list written in Step 1 — 92 constants,
in that order.

Then the normalizer and the exported guard:

```go
// normalizeRPCMethod bounds the label at record time.
func normalizeRPCMethod(m RPCMethod) RPCMethod {
	switch m {
	// Every constant declared above, in the same order as allRPCMethods.
	// TestValidAcceptsEveryDeclaredMethodAndNothingElse fails if one is missed —
	// an omitted method would panic every service that registers it.
	case MethodToggleMute, MethodToggleFavorite, MethodMoveChat, MethodOpenRoom,
		/* … the remaining 86 … */
		MethodSendRoomMessage, MethodSendDM, MethodGetPresenceSnapshot:
		return m
	default:
		return MethodUnknown
	}
}

// Valid reports whether m is a declared method, and is what natsrouter checks
// before it will register a route. Exported for that reason and no other: a
// caller that can build an RPCMethod from an arbitrary string is exactly the
// hole registration is meant to close, and the metric label must stay bounded
// whichever way the value arrives.
//
// MethodUnknown is deliberately not valid. It is the record-time fallback for a
// value that should never occur, not a method a route may claim.
func (m RPCMethod) Valid() bool {
	return m != MethodUnknown && normalizeRPCMethod(m) == m
}
```

`MethodUnknown` stays spelled `"unknown"` — see "Rejected: `_OTHER` as the
fallback value" above.

- [ ] **Step 4: Run the test to verify it passes**

Run: `make test SERVICE=pkg/natsmetrics`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/natsmetrics/rpcmethod.go pkg/natsmetrics/rpcmethod_test.go
git commit -m "feat(o11y): add the verb-first RPCMethod vocabulary"
```

---

### Task 2: Registration-time rpc.method — router API, instrument retype, mapper deletion

**Files:**
- Modify: `pkg/natsrouter/params.go:51-55`, `pkg/natsrouter/router.go:200-250`, `pkg/natsrouter/register.go`
- Modify: all 10 registration files (96 call sites)
- Test: `pkg/natsrouter/router_test.go`

**Interfaces:**
- Consumes: `natsmetrics.RPCMethod` and `RPCMethod.Valid()` from Task 1.
- Produces: `Register[Req, Resp](r *Router, pattern string, method natsmetrics.RPCMethod, fn func(*Context, Req) (*Resp, error))`; the same third-parameter shape for `RegisterNoBody` and `RegisterOptionalBody`; `RegisterVoid[Req](r *Router, pattern string, fn func(*Context, Req) error)` unchanged; `route.method natsmetrics.RPCMethod` plus `route.recordRPC bool`; `Router.methods map[natsmetrics.RPCMethod]string`.

**This task is atomic and cannot be split by service.** Changing the `Register`
signature breaks every call site at once; a partial migration does not compile.
Work service by service within the one commit.

**Two guarantees the type system alone does not give**, both enforced here as a
startup panic (the file already panics on a failed subscribe, so this is the
established failure mode for a misconfigured route):

1. A method must be **declared** — `RPCMethod("typo")` and `RPCMethod("")`
   compile fine, so `Valid()` is checked at registration.
2. A method must be **unique within this router** — two routes passing the same
   constant would merge into one series, which is the failure the vocabulary
   exists to remove. Uniqueness is per router, not per fleet: `service_name` +
   `rpc_method` is the identity key, so `mark_all_threads_read` is legitimately
   registered by both room-service and user-service.

There is no empty-string sentinel. `recordRPC` says whether a route records,
so a mistyped empty method cannot silently become fire-and-forget.

- [ ] **Step 1: Write the failing router tests**

Add to `pkg/natsrouter/router_test.go`. **Use the file's real harness** — there
is no fake-publisher option: `natsmetrics.Publisher` is a struct, not an
interface (`pkg/natsmetrics/metrics.go:599`), so `WithMetrics` only accepts one
built from a real meter provider. The established shape is at
`router_test.go:71-90`: `startTestNATS(t)` + `sdkmetric.NewManualReader()` +
`natsmetrics.NewFromProvider(mp).Publisher("site-a")`, driven with
`nc.Request(...)` and read back through `reader.Collect` inside
`require.Eventually`. `attrsOfPoint` (`router_test.go:864`) turns an
`attribute.Set` into a map.

```go
// serverCallMethods returns the rpc.method of every rpc.server.call.duration
// point recorded so far. Collect is a snapshot, so callers that expect zero
// samples must first wait on something the handler itself sets.
func serverCallMethods(t *testing.T, reader *sdkmetric.ManualReader) []string {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	var methods []string
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok || m.Name != "rpc.server.call.duration" {
				continue
			}
			for _, point := range hist.DataPoints {
				methods = append(methods, attrsOfPoint(point.Attributes)["rpc.method"])
			}
		}
	}
	return methods
}

// A reply-registering route records the method its registration named — not one
// derived from the subject, which is what let user-service's
// chatlist.section.create record as room-service's room_mutation.
func TestRouter_RecordsRegisteredMethod(t *testing.T) {
	nc := startTestNATS(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	r := New(nc, "room-service", WithMetrics(natsmetrics.NewFromProvider(mp).Publisher("site-a")))
	Register(r, "chat.user.{account}.request.room.{roomID}.site-a.room.rename",
		natsmetrics.MethodRenameRoom,
		func(_ *Context, req testReq) (*testResp, error) { return &testResp{Greeting: "ok"}, nil })

	_, err := nc.Request(context.Background(),
		"chat.user.alice.request.room.room-a.site-a.room.rename", []byte(`{"name":"ok"}`), 2*time.Second)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return assert.ObjectsAreEqual([]string{"rename_room"}, serverCallMethods(t, reader))
	}, time.Second, 10*time.Millisecond)
}

// A RegisterVoid route has no reply subject, so it is not an RPC call. It must
// produce no rpc.server.call.duration sample at all — its local handler time is
// not a round trip, and the presence heartbeat lane that uses this is the
// fleet's highest-volume traffic, so recording it inflates the histogram's count
// and pulls every percentile down.
//
// The handler signals through done, because there is no reply to wait on and
// asserting "still empty" against an unsynchronised handler would pass before
// the handler had run at all.
func TestRouter_RegisterVoidRecordsNoRPCSample(t *testing.T) {
	nc := startTestNATS(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	r := New(nc, "user-presence-service", WithMetrics(natsmetrics.NewFromProvider(mp).Publisher("site-a")))
	done := make(chan struct{})
	RegisterVoid(r, "chat.user.{account}.event.presence.site-a.ping",
		func(_ *Context, req testReq) error { close(done); return nil })

	require.NoError(t, nc.PublishMsg(context.Background(), &nats.Msg{
		Subject: "chat.user.alice.event.presence.site-a.ping",
		Data:    []byte(`{"name":"ok"}`),
	}))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("void handler never ran")
	}
	assert.Empty(t, serverCallMethods(t, reader))
}

// An undeclared method compiles — RPCMethod is a string type — so registration
// is where it has to be caught. Left unchecked it would record as "unknown",
// which is precisely the bucket this work removes.
func TestRegisterPanicsOnUndeclaredMethod(t *testing.T) {
	for _, method := range []natsmetrics.RPCMethod{"", "not_registered", natsmetrics.MethodUnknown} {
		t.Run(string(method), func(t *testing.T) {
			r := New(startTestNATS(t), "test")
			assert.Panics(t, func() {
				Register(r, "chat.user.{account}.request.room.{roomID}.site-a.open", method,
					func(_ *Context, req testReq) (*testResp, error) { return &testResp{}, nil })
			})
		})
	}
}

// Two routes sharing a method merge into one series, and nothing downstream can
// separate them again. The vocabulary test cannot see this — only a router knows
// which methods one service registered.
func TestRegisterPanicsOnDuplicateMethodInOneRouter(t *testing.T) {
	r := New(startTestNATS(t), "room-service")
	Register(r, "chat.user.{account}.request.room.{roomID}.site-a.open",
		natsmetrics.MethodOpenRoom,
		func(_ *Context, req testReq) (*testResp, error) { return &testResp{}, nil })

	assert.Panics(t, func() {
		Register(r, "chat.user.{account}.request.room.{roomID}.site-a.app.tabs",
			natsmetrics.MethodOpenRoom,
			func(_ *Context, req testReq) (*testResp, error) { return &testResp{}, nil })
	})
}

// Admission rejection replies before the handler runs, so it has no result to
// classify — but it is still that route's traffic and must carry its method, or
// a saturation incident lands in a different series from the route that
// saturated.
func TestRouter_AdmissionRejectionKeepsRouteMethod(t *testing.T) {
	nc := startTestNATS(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	r := New(nc, "room-service",
		WithMetrics(natsmetrics.NewFromProvider(mp).Publisher("site-a")), WithMaxConcurrency(1))
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	Register(r, "chat.user.{account}.request.room.{roomID}.site-a.open",
		natsmetrics.MethodOpenRoom,
		func(_ *Context, req testReq) (*testResp, error) { <-block; return &testResp{}, nil })

	// The first request occupies the only slot; the second is shed.
	go func() {
		_, _ = nc.Request(context.Background(),
			"chat.user.alice.request.room.room-a.site-a.open", []byte(`{"name":"ok"}`), 5*time.Second)
	}()
	require.Eventually(t, func() bool {
		_, err := nc.Request(context.Background(),
			"chat.user.bob.request.room.room-b.site-a.open", []byte(`{"name":"ok"}`), time.Second)
		return err == nil && len(serverCallMethods(t, reader)) > 0
	}, 3*time.Second, 20*time.Millisecond)

	assert.Contains(t, serverCallMethods(t, reader), "open_room")
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=pkg/natsrouter`
Expected: compile failure — `Register` takes 3 arguments, not 4.

- [ ] **Step 3: Carry the method and the record flag on `route`**

`pkg/natsrouter/params.go`:

```go
type route struct {
	natsSubject string         // "chat.user.*.request.room.*.*.msg.history"
	params      map[int]string // {2: "account", 5: "roomID", 6: "siteID"}
	// method is the bounded rpc.method this route records under, resolved once
	// at registration and validated there.
	method natsmetrics.RPCMethod
	// recordRPC separates "fire-and-forget" from "method not set". Without it an
	// empty method would silently mean void, so one mistyped argument would drop
	// a real route out of the RPC family with nothing to notice it.
	recordRPC bool
}
```

- [ ] **Step 4: Split `addRoute` into an RPC path and a void path**

In `pkg/natsrouter/router.go`, add `methods map[natsmetrics.RPCMethod]string` to
`Router` (method → the pattern that claimed it), and replace `addRoute`:

```go
// addRPCRoute registers a reply-bearing route under a declared, router-unique
// method. Both checks panic: a route is registered once at startup, and a
// service whose telemetry is mislabelled should fail to start rather than run
// blind.
func (r *Router) addRPCRoute(pattern string, method natsmetrics.RPCMethod, handlers []HandlerFunc) {
	if !method.Valid() {
		panic(fmt.Sprintf("natsrouter: route %s declares rpc method %q, which is not in the natsmetrics vocabulary", pattern, method))
	}
	r.mu.Lock()
	if claimed, dup := r.methods[method]; dup {
		r.mu.Unlock()
		panic(fmt.Sprintf("natsrouter: rpc method %q is already registered by %s; two routes sharing a method merge into one series", method, claimed))
	}
	if r.methods == nil {
		r.methods = map[natsmetrics.RPCMethod]string{}
	}
	r.methods[method] = pattern
	r.mu.Unlock()

	r.addRoute(pattern, method, true, handlers)
}

// addVoidRoute registers a fire-and-forget route. It has no reply subject, so
// there is no call to time and no method to name.
func (r *Router) addVoidRoute(pattern string, handlers []HandlerFunc) {
	r.addRoute(pattern, "", false, handlers)
}

func (r *Router) addRoute(pattern string, method natsmetrics.RPCMethod, recordRPC bool, handlers []HandlerFunc) {
	rt := parsePattern(pattern)
	rt.method = method
	rt.recordRPC = recordRPC
	...
```

Note the `r.methods == nil` check must run before the map write; initialise the
map in `New` instead if that reads better in context.

- [ ] **Step 5: Record through a helper that honours `recordRPC`**

Delete the `operation := natsmetrics.RequestOperationFromSubject(m.Subject)`
line from `natsHandler` and add:

```go
// recordHandled reports one handler result, or nothing at all for a
// fire-and-forget route. Routed through one helper so the three record sites
// (stopping gate, admission rejection, handler completion) cannot disagree
// about whether a void route is recorded.
func (r *Router) recordHandled(ctx context.Context, rt route, started time.Time, result natsmetrics.RequestResult) {
	if !rt.recordRPC {
		return
	}
	r.metrics.HandledRequest(ctx, rt.method, time.Since(started), result)
}
```

Replace all three `r.metrics.HandledRequest(...)` calls in `natsHandler` with
`r.recordHandled(msgCtx, rt, started, …)`.

- [ ] **Step 6: Add the parameter to the three reply-registering functions**

In `pkg/natsrouter/register.go`, for `Register`, `RegisterNoBody` and
`RegisterOptionalBody`, insert `method natsmetrics.RPCMethod` after `pattern`
and call `addRPCRoute`:

```go
func Register[Req, Resp any](
	r *Router,
	pattern string,
	method natsmetrics.RPCMethod,
	fn func(c *Context, req Req) (*Resp, error),
) {
	handler := HandlerFunc(func(c *Context) { /* unchanged */ })
	r.addRPCRoute(pattern, method, []HandlerFunc{handler})
}
```

`RegisterVoid` keeps its signature and calls `addVoidRoute`, with the reason
stated where a reader will look for it:

```go
// RegisterVoid subscribes a handler that processes a request without replying.
//
// It takes no rpc.method and records no rpc.server.call.duration sample: with
// no reply subject there is no call to time, and the presence heartbeat lane
// that uses this is the fleet's highest-volume traffic, so recording it would
// inflate the histogram's count and pull every percentile down. Observe these
// routes with a handler-duration metric of their own, not the RPC family.
func RegisterVoid[Req any](r *Router, pattern string, fn func(c *Context, req Req) error) {
	handler := HandlerFunc(func(c *Context) { /* unchanged */ })
	r.addVoidRoute(pattern, []HandlerFunc{handler})
}
```

- [ ] **Step 7: Migrate all 96 call sites**

Work through the files in this order, inserting the constant from the naming
table as the third argument. After each file, run that service's tests.

| # | File | Routes | Verify with |
|---|---|---|---|
| 1 | `room-service/handler.go` | 27 | `make test SERVICE=room-service` |
| 2 | `history-service/internal/service/service.go` | 17 | `make test SERVICE=history-service` |
| 3 | `user-service/service/service.go` | 29 | `make test SERVICE=user-service` |
| 4 | `search-service/handler.go` | 5 | `make test SERVICE=search-service` |
| 5 | `bot-room-service/handler.go` | 5 | `make test SERVICE=bot-room-service` |
| 6 | `user-presence-service/main.go` | 3 (+4 `RegisterVoid`, unchanged) | `make test SERVICE=user-presence-service` |
| 7 | `media-service/emoji_nats.go` | 2 | `make test SERVICE=media-service` |
| 8 | `bot-message-handler/handler.go` | 2 | `make test SERVICE=bot-message-handler` |
| 9 | `translation-service/main.go` | 1 | `make test SERVICE=translation-service` |
| 10 | `room-worker/main.go` | 1 | `make test SERVICE=room-worker` |

`natsrouter`'s own tests register routes too, and they are not optional: 34
calls across `router_test.go` (19), `example_test.go` (6), `shutdown_test.go`
(5) and `middleware_test.go` (4). One of them also asserts the old label —
`router_test.go:114` requires `attrs["rpc.method"] == "member_read"` for the
`member.list` route, which becomes `list_members`.

**Ten more call sites are behind `//go:build integration` and neither `make
lint` nor `make test` compiles them:** `pkg/natsrouter/integration_test.go` (7),
`pkg/natsrouter/oversize_integration_test.go` (2), `room-service/integration_test.go`
(7), `translation-service/integration_test.go` (1). Migrate them in this task —
a missed one is invisible until CI runs the integration suite.

The history-service mutation and migration routes wrap the service method in a
closure; the constant goes before the closure:

```go
natsrouter.Register(r, subject.MsgEditPattern(siteID), natsmetrics.MethodEditMessage,
	func(c *natsrouter.Context, req models.EditMessageRequest) (*models.EditMessageResponse, error) {
		return s.EditMessage(c, siteID, req)
	})
```

Everywhere else the handler is passed directly:

```go
natsrouter.Register(r, subject.RoomRenamePattern(h.siteID), natsmetrics.MethodRenameRoom, h.roomRename)
```

- [ ] **Step 8: Run the unit gates**

Run: `make test SERVICE=pkg/natsrouter && make lint`
Expected: PASS, 0 lint issues.

- [ ] **Step 9: Compile the integration-tagged call sites**

`make lint` and `make test` both skip `//go:build integration` files, so the ten
calls listed in Step 7 are still unverified at this point.

Run: `make test-integration SERVICE=pkg/natsrouter`, then `SERVICE=room-service`
and `SERVICE=translation-service`.
Expected: PASS. These need Docker (testcontainers). **If Docker is unavailable,
say so explicitly in the task report and leave this leg to CI** — do not claim
the call sites are verified.

**Merged from the plan's original Task 3 by controller ruling.** The router's
record call, the subject mapper's return type and the instrument's parameter
type are one atomic unit: retyping `HandledRequest` to `RPCMethod` in a later
task leaves this task passing an `RPCMethod` to an `Operation` parameter, so
its own Step 8 (`make lint`, 0 issues) could never pass. What stays behind in
Task 3 is only the publish-side rename, which nothing here depends on.

- [ ] **Step 10: Retype the two RPC instruments**

**Do not rename `Operation` here** — that is Task 3, and nothing in this task
depends on it. Only the two RPC instruments and their keys move to `RPCMethod`.

In `pkg/natsmetrics/metrics.go`: delete every RPC constant
(the whole block added in `2f5bfe4`, plus `OperationHistoryRead`,
`OperationHistoryMutation`, `OperationRoomRead`, `OperationRoomMutation`,
`OperationMemberRead`, `OperationMemberMutation`, `OperationTeamsRoom`,
`OperationChannelHistory`, `OperationThreadOpen`, `OperationHistoryGetMessage`,
`OperationPresenceLookup`), rename `normalizeOperation` to
`normalizePublishOperation`, and retype the keys:

```go
type requestKey struct {
	method  RPCMethod
	outcome PublishOutcome
}

type handledRequestKey struct {
	method RPCMethod
	result RequestResult
}
```

Update `Publisher.Request` and `Publisher.HandledRequest` to take `RPCMethod`
and call `normalizeRPCMethod`; `Publisher.Failure` keeps `PublishOperation` and
`normalizePublishOperation`. `rpcMethod()` in `rpcsemconv.go` takes `RPCMethod`.

- [ ] **Step 11: Delete the subject-derived mapper and everything that only served it**

From `pkg/natsmetrics/subject.go` delete `RequestOperationFromSubject`,
`userOperation`, `searchOperation`, `emojiOperation`, `botOperation`,
`isBotRequest`, `isPresenceQuery`, `isRoomFamilyRequest`, `isRequestFamily`,
`isMigrationFamily`, **and `hasAnySuffix`** — all sixteen of its call sites live
inside those functions, and `.golangci.yml` runs the `unused` linter, so leaving
it makes `make lint` fail. Keep `EventTypeFromSubject`, `RoomEventTypeFromSubject`,
`PublishLabelsFromSubject`, and `isMemberEventAt` (still called at
`subject.go:289,295`). Retype `PublishLabelsFromSubject` to return
`PublishOperation`.

From `pkg/natsmetrics/subject_test.go` delete three whole functions:
`TestRequestOperationFromSubject`,
`TestRequestOperationFromSubject_DoesNotCrossServiceFamilies`, and
`TestRequestOperationFromSubject_NonRequestSubjectsAreUnknown`.

```bash
git rm pkg/natsmetrics/routecoverage_test.go
```

- [ ] **Step 12: Fix the natsmetrics test files that drive the retyped instruments**

None of these compile after Step 3, and none is reachable by grepping for the
type name — they name the constants:

| File | What to change |
|---|---|
| `pkg/natsmetrics/rpcsemconv_test.go` | 15 uses of `OperationHistoryRead` / `OperationRoomRead` → `MethodGetChannelHistory` / `MethodOpenRoom` |
| `pkg/natsmetrics/toggle_test.go:35-36` | `OperationHistoryRead`, `OperationMemberRead` → an `RPCMethod` and a `PublishOperation` respectively, matching which instrument each line drives |
| `pkg/natsmetrics/metrics_test.go:243,271` | `OperationPresenceLookup` → `MethodGetPresenceSnapshot` |
| `pkg/natsmetrics/metrics_test.go:255-256` | two dynamic values; the one passed to `Request` becomes `RPCMethod("dynamic.operation")`, the other stays a `PublishOperation` |
| `pkg/natsmetrics/prometheus_export_test.go:61-62` | `OperationPresenceLookup`, `OperationRoomRead` → `MethodGetPresenceSnapshot`, `MethodOpenRoom` |

- [ ] **Step 13: Migrate room-service's client interface and the six client call sites**

Only two production files name the type in a signature, and **they need
different target types** — a blanket search-and-replace to `PublishOperation`
breaks room-service, because its interface is the client RPC path:

| File | Line | New type |
|---|---|---|
| `room-service/memberlist_client.go` | 38-40, the `requestRecorder` interface | `natsmetrics.RPCMethod` |
| `history-service/internal/publisher/publisher.go` | 18, the `Failure` interface | `natsmetrics.PublishOperation` |

Then the six `Publisher.Request` call sites, using the table in the Naming
section: `room-service/reader_history.go:70`,
`message-gatekeeper/fetcher_history.go:83`,
`broadcast-worker/parent_fetcher.go:62`,
`notification-worker/parent_fetcher.go:76`,
`room-service/memberlist_client.go:96`, `notification-worker/presence.go:81`.

Publish call sites that merely pass a constant need no edit — the constants keep
their names.

- [ ] **Step 14: Regenerate room-service's mock**

Two generated mocks encode the signatures just changed
(`room-service/mock_memberlist_client_test.go:86`,
`history-service/internal/publisher/mock_publisher_test.go:45`). CLAUDE.md
forbids hand-editing them.

Run: `make generate SERVICE=room-service` and
`make generate SERVICE=history-service`.

- [ ] **Step 15: Commit**

```bash
git add pkg/natsrouter room-service history-service user-service search-service \
  media-service translation-service user-presence-service bot-room-service \
  bot-message-handler room-worker
git commit -m "feat(natsrouter): require a valid, unique rpc.method at registration"
```

---

### Task 3: Rename Operation to PublishOperation

**Files:**
- Modify: `pkg/natsmetrics/metrics.go`, `pkg/natsmetrics/subject.go`, `pkg/natsmetrics/subject_test.go`, `pkg/natsmetrics/enums_test.go`, `pkg/natsmetrics/opttable.go`
- Modify: the 6 client call sites and the publish call sites listed in File Structure
- Delete: `pkg/natsmetrics/routecoverage_test.go`

**Interfaces:**
- Consumes: `RPCMethod` from Task 1; `route.method` from Task 2.
- Produces: `type PublishOperation string`; `func (p Publisher) Request(ctx context.Context, method RPCMethod, duration time.Duration, err error)`; `func (p Publisher) HandledRequest(ctx context.Context, method RPCMethod, duration time.Duration, result RequestResult)`; `func (p Publisher) Failure(ctx context.Context, destination DestinationKind, operation PublishOperation, err error)`; `func PublishLabelsFromSubject(subj string) (DestinationKind, PublishOperation)`.

- [ ] **Step 1: Write the failing budget and separation tests**

The rename touches four things in `pkg/natsmetrics/enums_test.go`, not one.
Miss any and the file does not compile:

1. `allOperations` becomes `allPublishOperations`, publish-only (below).
2. `TestNormalizersAcceptEveryDeclaredValue` loops it and calls
   `normalizeOperation` — retarget both to the renamed symbols.
3. The `publishPair` struct field `operation Operation` (line 159) becomes
   `PublishOperation`.
4. `TestPublishLabelPairsAreFarNarrowerThanTheCrossProduct` (line 155) uses
   `len(allOperations)`.

The publish cross-product now measures only operations publish can actually
emit, so it means something again:

```go
	allPublishOperations = []PublishOperation{
		OperationCanonicalPublish, OperationClientResponse, OperationRecipientPublish,
		OperationNotificationPublish, OperationPushPublish, OperationThreadTCount,
		OperationTeamsUserUpsert, OperationRoomPublish, OperationMemberPublish,
		OperationOutboxPublish, OperationUnknown,
	}
```

```go
// Splitting RPCMethod out of Operation is what makes these three numbers
// independent. Before the split one enum fed all three families, so adding a
// request method inflated the publish cross-product by 84 series it could never
// reach — the assert stopped describing anything real.
func TestLabelSpaceStaysWithinBudget(t *testing.T) {
	assert.Equal(t, 75, len(allEventTypes)*len(allOutcomes),
		"chat.nats.consumer.messages / .processing.duration: event_type x outcome")
	assert.Equal(t, 105, len(allEventTypes)*len(allTerminalReasons),
		"chat.nats.terminal.failures: event_type x reason")
	assert.Equal(t, 924, len(allDestinations)*len(allPublishOperations)*len(allPublishOutcomes),
		"chat.nats.publish.failures: destination_kind x operation x outcome")
	// +1 is MethodUnknown, which is not a registered method (so it is absent
	// from allRPCMethods and exempt from the naming rule) but is still a value
	// the normalizer can emit, and therefore a series the backend can hold.
	assert.Equal(t, 744, (len(allRPCMethods)+1)*len(allRequestOutcomes),
		"rpc.client.call.duration: rpc.method x error.type, plus one unlabelled success series per method")
	assert.Equal(t, 837, (len(allRPCMethods)+1)*len(allRequestResults),
		"rpc.server.call.duration: rpc.method x error.type, plus one unlabelled success series per method")
}
```

The RPC figures are the honest worst case for the whole fleet, and no single
service reaches them: a process only registers its own routes, so user-service
tops out at 29 methods × 9 results. natsrouter's duplicate check is what makes
"its own routes" a real bound rather than a hope.

Add a test that the mapper is gone:

```go
// rpc.method comes from route registration. A subject-derived fallback would
// reintroduce exactly the cross-service misclassification the registration
// argument removes, so there must be no such function to call.
func TestNoSubjectDerivedRPCMethodRemains(t *testing.T) {
	src, err := os.ReadFile("subject.go")
	require.NoError(t, err)
	assert.NotContains(t, string(src), "RequestOperationFromSubject")
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `make test SERVICE=pkg/natsmetrics`
Expected: compile failure — `undefined: PublishOperation`, and the mapper still present.

- [ ] **Step 3: Rename the publish enum and retype PublishLabelsFromSubject**

In `pkg/natsmetrics/metrics.go` rename the type `Operation` to
`PublishOperation` and `normalizeOperation` to `normalizePublishOperation`,
and delete every RPC constant left unused by Task 2
(`OperationHistoryRead`, `OperationHistoryMutation`, `OperationRoomRead`,
`OperationRoomMutation`, `OperationMemberRead`, `OperationMemberMutation`,
`OperationTeamsRoom`, `OperationChannelHistory`, `OperationThreadOpen`,
`OperationHistoryGetMessage`, `OperationPresenceLookup`, and the block added
in `2f5bfe4`). Retype `PublishLabelsFromSubject` to return
`PublishOperation`.

- [ ] **Step 4: Migrate history-service's publisher interface and regenerate its mock**

`history-service/internal/publisher/publisher.go:18` declares
`Failure(context.Context, natsmetrics.DestinationKind, natsmetrics.Operation, error)`
→ `natsmetrics.PublishOperation`. Then run
`make generate SERVICE=history-service`; `mock_publisher_test.go:45` encodes
the old signature and CLAUDE.md forbids hand-editing it.

- [ ] **Step 5: Run the tests**

Run: `make test SERVICE=pkg/natsmetrics && make lint`
Expected: PASS, 0 lint issues. `make lint` is the repo-wide compile gate.

- [ ] **Step 6: Run the full unit suite with race**

Run: `make test`
Expected: no `FAIL`.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor(o11y): rename Operation to PublishOperation"
```

---

### Task 4: Prometheus export guard

**Files:**
- Modify: `pkg/natsmetrics/prometheus_export_test.go`

**Interfaces:**
- Consumes: everything from Tasks 1-3.
- Produces: no production symbols.

- [ ] **Step 1: Write the failing export test**

```go
// The exported label must never carry "unknown" for traffic a route served:
// registration makes the method mandatory, so an unknown here means a caller
// built an RPCMethod from a string and the normalizer rejected it.
func TestExportedRPCMethodIsNeverUnknownForRegisteredTraffic(t *testing.T) {
	reg, publisher := newPrometheusPublisher(t)
	publisher.HandledRequest(context.Background(), MethodRenameRoom, 5*time.Millisecond, RequestSuccess)
	publisher.HandledRequest(context.Background(), MethodGetMessage, 5*time.Millisecond, RequestNotFound)

	families := gather(t, reg)
	methods := labelValues(families, "rpc_server_call_duration_seconds", "rpc_method")
	assert.ElementsMatch(t, []string{"rename_room", "get_message"}, methods)
	assert.NotContains(t, methods, "unknown")
}

// Every sample carries a non-empty rpc_method. The label is required at
// registration, so its absence here would mean a record path bypassed the
// route — the one way an unlabelled series could still appear.
//
// This file deliberately does NOT assert that a method maps to one handler:
// nothing in pkg/natsmetrics knows what a handler is. That guarantee is
// natsrouter's, enforced by the per-router duplicate panic and covered by
// TestRegisterPanicsOnDuplicateMethodInOneRouter.
func TestEveryExportedSampleCarriesAMethod(t *testing.T) {
	reg, publisher := newPrometheusPublisher(t)
	for _, m := range []RPCMethod{MethodRenameRoom, MethodGetMessage, MethodSendDM} {
		publisher.HandledRequest(context.Background(), m, time.Millisecond, RequestSuccess)
	}

	families := gather(t, reg)
	methods := labelValues(families, "rpc_server_call_duration_seconds", "rpc_method")
	require.Len(t, methods, 3)
	for _, m := range methods {
		assert.NotEmpty(t, m)
	}
}
```

**The helper names above do not exist.** `pkg/natsmetrics/prometheus_export_test.go`
has exactly one: `newPrometheusExportSetup(t) (*Metrics, *prometheus.Registry)`
— a different name, a different return order, and it hands back `*Metrics`, not
a `Publisher`. Build the publisher from it the way the file's existing tests do,
and add whatever gather/label helpers are missing alongside them rather than
assuming the shape sketched here drops in. `uniqueStrings` is not needed — the
list-level duplicate check lives in `rpcmethod_test.go` and the per-router one
in `pkg/natsrouter`.

- [ ] **Step 2: Run to verify it fails**

Run: `make test SERVICE=pkg/natsmetrics`
Expected: FAIL on the missing helpers or the assertion.

- [ ] **Step 3: Implement the helpers and make it pass**

Add only the test helpers; no production code changes.

- [ ] **Step 4: Run to verify it passes**

Run: `make test SERVICE=pkg/natsmetrics`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/natsmetrics/prometheus_export_test.go
git commit -m "test(o11y): assert the exported rpc_method is bounded and one-to-one"
```

---

### Task 5: Documentation and the metric-rename runbook

**Files:**
- Modify: `docs/specs/o11y/nats-metrics-contract.md` (§2 label list, §13.1)
- Modify: `docs/load-testing/common/sli-slo.md` (§3 SLO-4/5 queries, §8 P1 row)
- Modify: `docs/specs/o11y/o11y-metrics-inventory.md:196-200` — lists the full old operation enum and says `channel_history`/`thread_open` exist to serve SLO-4/5
- Modify: `docs/load-testing/loadgen/observation.md:156` — a table row mapping `room_mutation` to `room_rename, mute_toggle`

**Interfaces:**
- Consumes: the naming table and the SLO analysis in this plan.
- Produces: no code.

- [ ] **Step 1: Rewrite the contract's §13.1 `rpc.method` section**

Replace the coverage note with the new mechanism. State: `rpc.method` is
supplied at route registration and required by `Register`, `RegisterNoBody` and
`RegisterOptionalBody`, so a route without one does not compile; `RegisterVoid`
takes none and records no sample, which is why the presence heartbeat lane no
longer appears in this family at all; the naming rule is
`<verb>_<object>[_qualifier]` in lower `snake_case`, guarded by
`pkg/natsmetrics/rpcmethod_test.go`; `service_name` + `rpc_method` resolves to
exactly one handler, enforced by natsrouter's per-router duplicate check. Record
the three orphans found during this work:
`chat.server.request.room.{site}.key.ensure` and
`chat.server.bot.request.room.{site}.get` have no caller in the repo, and
`chat.presence.{site}.request.snapshot` has a caller but no subscriber.

- [ ] **Step 2: Update the SLO queries**

In `docs/load-testing/common/sli-slo.md`, change `rpc_method="thread_open"` to
`rpc_method="get_thread_messages"` and `rpc_method="channel_history"` to
`rpc_method="get_channel_history"` in the SLO-4 and SLO-5 PromQL blocks. No
bridging rule: the SLI/SLO programme is not live, so nothing is measuring these
labels yet and there is no window to preserve. The edit is so the document is
correct on the day the SLOs launch.

- [ ] **Step 3: Add the cutover note**

Under the §8 P1 row, add: every `rpc_method` value changed with the move to
registration-time methods. Grafana panels filtering on `room_mutation`,
`room_read`, `member_read`, `member_mutation`, `teams_room`, `history_read`,
`history_mutation`, `channel_history` or `thread_open` need a pass — a
dashboard-owner task, not a rollout blocker. The `rpc_method="unknown"` series
disappears entirely rather than shrinking, so a panel excluding it should drop
the filter rather than leave it dangling.

- [ ] **Step 4: Verify the docs match the code**

Run: `grep -rn 'channel_history\|thread_open\|room_mutation\|history_read' docs/`
Expected: hits only inside the cutover note, where they are named as the old
values a dashboard owner must replace — never as a current label value. All four
files in this task's Modify list must be clean; `o11y-metrics-inventory.md` and
`observation.md` carry the old vocabulary and are the two easiest to forget. The
plans directory is exempt: `docs/superpowers/plans/` is a historical record and
its older plans legitimately quote the labels of their own time.

- [ ] **Step 5: Commit**

```bash
git add docs/specs/o11y/nats-metrics-contract.md docs/load-testing/common/sli-slo.md
git commit -m "docs(o11y): document registration-time rpc.method and the label cutover"
```

---

### Task 6: Squash the branch to one commit and push

**Files:** none.

**Interfaces:** none.

The branch carries `2f5bfe4`, which added the family-based vocabulary this plan
replaces, plus the plan commits. `2f5bfe4`'s `routecoverage_test.go` and mapper
changes are deleted by Task 3, so leaving it in history means a reviewer reads a
design that never shipped. The branch has no PR. Ship one commit.

- [ ] **Step 1: Confirm the branch is solely ours**

```bash
git log --format='%an' "$(git merge-base origin/main HEAD)"..HEAD | sort -u
```

Scope matters: without the merge-base bound this walks all of `main`'s history
and returns every author who ever touched the repo, which would stop the task
every time. Expected: one author. **If anyone else appears, stop and ask** — do
not rewrite another author's commits.

- [ ] **Step 2: Squash onto the base**

```bash
git fetch origin main
git reset --soft "$(git merge-base origin/main HEAD)"
```

The working tree is untouched; every change from Tasks 1-5 is staged as one
change set, and `2f5bfe4` is gone from the branch.

- [ ] **Step 3: Verify from the squashed state**

```bash
make lint
make test
make sast-gosec
make sast-semgrep-test
```

Expected: 0 lint issues, no `FAIL`, gosec PASS, semgrep rule tests pass.
`make sast` also runs `govulncheck` and pulls semgrep's remote `p/golang`
ruleset; both are blocked by proxy policy in this container (403 to
`vuln.go.dev` and `semgrep.dev`), so **CI must run those two legs** — do not
report the SAST gate as green.

- [ ] **Step 4: Commit and force-push with lease**

```bash
git commit   # one message covering the whole change; see below
git push --force-with-lease -u origin claude/nats-rpc-operation-vocab-slbows
```

The message should say what changed and why in this order: `rpc_method` moves
from subject inference to a required registration argument, so a route without
one does not compile and a duplicate panics at startup; `RegisterVoid` records
nothing, which removes the presence heartbeat lane from the RPC family
entirely; names follow `<verb>_<object>`; `Operation` splits into
`PublishOperation` and `RPCMethod`; every `rpc_method` value changes, and no SLO
bridging was needed because the SLI/SLO programme is not live.

- [ ] **Step 5: Report**

State the verification actually run, name the two SAST legs CI still owes, and
say that Grafana panels filtering on the old family labels need a dashboard
pass.

---

## Out of Scope

Deliberately not in this plan, each already analysed and parked:

- **`rpc.client` (caller identity).** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.get` has five caller classes — the frontend plus room-service, message-gatekeeper, broadcast-worker and notification-worker — and is the only route in the repo whose subject cannot separate them. Two options exist: a server-lane twin subject (`chat.server.request.history.{site}.msg.get`, which also lets ops narrow those four services' `chat.user.>` publish permission), or a bounded caller header. Neither belongs in this PR.
- **Filling the nine uninstrumented `Publisher.Request` call sites** in `user-service/roomclient` (4), `user-service/historyclient` (2), `user-service/presenceclient` (1), `search-service/room_client.go` and `notification-worker/badge_client.go`.
- **A handler-duration metric for `RegisterVoid` routes.** This plan stops recording them in the RPC family; giving them a home of their own is separate.
- **`HandlerTimeout` collapsing to `error_type="internal"`.** `requestResultFromError` does not test `context.DeadlineExceeded`, so a service hitting its handler budget is indistinguishable from a bug. A one-line fix in `pkg/natsrouter/context.go`, but it changes the `error_type` value set and therefore the SLO denominator.
- **The `le=10` bucket ceiling.** `o11y.DefaultLatencyBuckets()` tops out at 10s, so `histogram_quantile` reports exactly 10 for any true p99 above it. A dashboard concern with a company-wide bucket decision behind it.
