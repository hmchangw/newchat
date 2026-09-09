package natsmetrics

// RPCMethod is the rpc.method label on rpc_server_call_duration_seconds and its
// client twin. It is a closed vocabulary, declared at route registration rather
// than derived from the NATS subject: the old subject parser only recognised the
// room and orgs families, so 51 of 96 routes recorded as "unknown" — every
// user-service, search, presence, translation, media and bot route among them.
//
// Names are verb-first snake_case, and which verb is right is a semantic rule no
// test can check, so it is written down here. Following AIP-131/132/190:
//
//	get_        one logical resource
//	list_       a collection, whether or not it carries a total
//	batch_get_  several specific resources by caller-supplied keys
//	search_     a query
//
// A method names what the handler does, not what its subject happens to spell.
// Where the two disagree the handler wins — mark_room_read advances a room read
// position and carries no message id anywhere on its path, despite living under
// a .message. subject.
//
// One value is deliberately shared across two services: mark_all_threads_read is
// both user-service's client-facing route and the room-service route user-service
// calls to serve it. They are one logical operation over two hops, and a panel
// filtered on the method shows both.
type RPCMethod string

const (
	MethodAddBotRoomMembers            RPCMethod = "add_bot_room_members"
	MethodAddMembers                   RPCMethod = "add_members"
	MethodAddPriorityContact           RPCMethod = "add_priority_contact"
	MethodBatchGetBadgeCounts          RPCMethod = "batch_get_badge_counts"
	MethodBatchGetMessages             RPCMethod = "batch_get_messages"
	MethodBatchGetPeerPresence         RPCMethod = "batch_get_peer_presence"
	MethodBatchGetPresence             RPCMethod = "batch_get_presence"
	MethodBatchGetRoomPreviews         RPCMethod = "batch_get_room_previews"
	MethodBatchGetRoomsInfo            RPCMethod = "batch_get_rooms_info"
	MethodBatchGetThreadRoomsInfo      RPCMethod = "batch_get_thread_rooms_info"
	MethodCountSubscriptions           RPCMethod = "count_subscriptions"
	MethodCreateBotRoom                RPCMethod = "create_bot_room"
	MethodCreateChatlistSection        RPCMethod = "create_chatlist_section"
	MethodCreateDMRoom                 RPCMethod = "create_dm_room"
	MethodCreateRoom                   RPCMethod = "create_room"
	MethodCreateTeamsMeeting           RPCMethod = "create_teams_meeting"
	MethodDeleteChatlistSection        RPCMethod = "delete_chatlist_section"
	MethodDeleteEmoji                  RPCMethod = "delete_emoji"
	MethodDeleteMessage                RPCMethod = "delete_message"
	MethodEditMessage                  RPCMethod = "edit_message"
	MethodEnsureBotDMRoom              RPCMethod = "ensure_bot_dm_room"
	MethodEnsureRoomKey                RPCMethod = "ensure_room_key"
	MethodGetBotRoom                   RPCMethod = "get_bot_room"
	MethodGetChatlist                  RPCMethod = "get_chatlist"
	MethodGetCurrentUser               RPCMethod = "get_current_user"
	MethodGetDMSubscription            RPCMethod = "get_dm_subscription"
	MethodGetMessage                   RPCMethod = "get_message"
	MethodGetRoomAppCommandMenu        RPCMethod = "get_room_app_command_menu"
	MethodGetRoomAppTabs               RPCMethod = "get_room_app_tabs"
	MethodGetRoomKey                   RPCMethod = "get_room_key"
	MethodGetSettings                  RPCMethod = "get_settings"
	MethodGetSubscriptionByRoom        RPCMethod = "get_subscription_by_room"
	MethodGetThreadUnreadSummary       RPCMethod = "get_thread_unread_summary"
	MethodGetUserProfile               RPCMethod = "get_user_profile"
	MethodGetUserStatus                RPCMethod = "get_user_status"
	MethodListAppCategories            RPCMethod = "list_app_categories"
	MethodListApps                     RPCMethod = "list_apps"
	MethodListChannelMessages          RPCMethod = "list_channel_messages"
	MethodListChannelSubscriptions     RPCMethod = "list_channel_subscriptions"
	MethodListEmojis                   RPCMethod = "list_emojis"
	MethodListMemberStatuses           RPCMethod = "list_member_statuses"
	MethodListMembers                  RPCMethod = "list_members"
	MethodListMentionableSubscriptions RPCMethod = "list_mentionable_subscriptions"
	MethodListMessageReaders           RPCMethod = "list_message_readers"
	MethodListNextMessages             RPCMethod = "list_next_messages"
	MethodListOrgMembers               RPCMethod = "list_org_members"
	MethodListPinnedMessages           RPCMethod = "list_pinned_messages"
	MethodListPriorityContacts         RPCMethod = "list_priority_contacts"
	MethodListSubscriptions            RPCMethod = "list_subscriptions"
	MethodListSurroundingMessages      RPCMethod = "list_surrounding_messages"
	MethodListThreadMessages           RPCMethod = "list_thread_messages"
	MethodListThreadParentMessages     RPCMethod = "list_thread_parent_messages"
	MethodListThreadSubscriptions      RPCMethod = "list_thread_subscriptions"
	MethodListUserThreads              RPCMethod = "list_user_threads"
	MethodMarkAllThreadsRead           RPCMethod = "mark_all_threads_read"
	MethodMarkRoomRead                 RPCMethod = "mark_room_read"
	MethodMarkRoomThreadsRead          RPCMethod = "mark_room_threads_read"
	MethodMarkThreadRead               RPCMethod = "mark_thread_read"
	MethodMigrateDeleteMessage         RPCMethod = "migrate_delete_message"
	MethodMigrateEditMessage           RPCMethod = "migrate_edit_message"
	MethodMoveChat                     RPCMethod = "move_chat"
	MethodOpenRoom                     RPCMethod = "open_room"
	MethodPinMessage                   RPCMethod = "pin_message"
	// #nosec G101 -- an rpc.method label value, not a credential: it names the route that refreshes or sets a token, and is exported to Prometheus as a metric dimension
	MethodRefreshSSOToken            RPCMethod = "refresh_sso_token"
	MethodRemoveBotRoomMembers       RPCMethod = "remove_bot_room_members"
	MethodRemoveMember               RPCMethod = "remove_member"
	MethodRemovePriorityContact      RPCMethod = "remove_priority_contact"
	MethodRenameChatlistSection      RPCMethod = "rename_chatlist_section"
	MethodRenameRoom                 RPCMethod = "rename_room"
	MethodReorderChatlistSections    RPCMethod = "reorder_chatlist_sections"
	MethodSearchApps                 RPCMethod = "search_apps"
	MethodSearchMessages             RPCMethod = "search_messages"
	MethodSearchOrgs                 RPCMethod = "search_orgs"
	MethodSearchRooms                RPCMethod = "search_rooms"
	MethodSearchUsers                RPCMethod = "search_users"
	MethodSendDM                     RPCMethod = "send_dm"
	MethodSendRoomMessage            RPCMethod = "send_room_message"
	MethodSetAppSubscription         RPCMethod = "set_app_subscription"
	MethodSetChatlistSectionSortMode RPCMethod = "set_chatlist_section_sort_mode"
	MethodSetManualPresence          RPCMethod = "set_manual_presence"
	MethodSetRoomRestricted          RPCMethod = "set_room_restricted"
	MethodSetSettings                RPCMethod = "set_settings"
	// #nosec G101 -- an rpc.method label value, not a credential: it names the route that refreshes or sets a token, and is exported to Prometheus as a metric dimension
	MethodSetSSOToken           RPCMethod = "set_sso_token"
	MethodSetUserStatus         RPCMethod = "set_user_status"
	MethodStartTeamsRoomCall    RPCMethod = "start_teams_room_call"
	MethodStartTeamsUserCall    RPCMethod = "start_teams_user_call"
	MethodToggleFavorite        RPCMethod = "toggle_favorite"
	MethodToggleMessageReaction RPCMethod = "toggle_message_reaction"
	MethodToggleMute            RPCMethod = "toggle_mute"
	MethodTranslateText         RPCMethod = "translate_text"
	MethodUnpinMessage          RPCMethod = "unpin_message"
	MethodUpdateMemberRole      RPCMethod = "update_member_role"
)

// MethodOther is the record-time fallback for a method outside the vocabulary.
// semconv v1.40.0 makes "_OTHER" normative for an unrecognised rpc.method, and
// addRPCRoute degrades to it rather than panicking: a telemetry label defect
// should not stop a process, and metrics are opt-in, so a panic could kill a
// service over a value it might not even record. It is deliberately not Valid(),
// so no route can claim it and the fallback keeps meaning "should never happen".
const MethodOther RPCMethod = "_OTHER"

// MethodNone marks a route that records no rpc.server.call.duration sample.
// RegisterVoid routes carry it: a void handler sends no reply, so there is no
// round trip to time, and recording local handler cost under a call-duration
// histogram would misreport what the metric means. It is the zero RPCMethod so
// the intent is explicit at the call site rather than an omitted argument.
const MethodNone RPCMethod = ""

// rpcMethodVocabulary is the closed set Valid() reports. A map rather than a
// switch so rpcMethodVocabularySize can report its length to the completeness
// test, which is what catches a constant added here but not to allRPCMethods.
var rpcMethodVocabulary = map[RPCMethod]struct{}{
	MethodAddBotRoomMembers:            {},
	MethodAddMembers:                   {},
	MethodAddPriorityContact:           {},
	MethodBatchGetBadgeCounts:          {},
	MethodBatchGetMessages:             {},
	MethodBatchGetPeerPresence:         {},
	MethodBatchGetPresence:             {},
	MethodBatchGetRoomPreviews:         {},
	MethodBatchGetRoomsInfo:            {},
	MethodBatchGetThreadRoomsInfo:      {},
	MethodCountSubscriptions:           {},
	MethodCreateBotRoom:                {},
	MethodCreateChatlistSection:        {},
	MethodCreateDMRoom:                 {},
	MethodCreateRoom:                   {},
	MethodCreateTeamsMeeting:           {},
	MethodDeleteChatlistSection:        {},
	MethodDeleteEmoji:                  {},
	MethodDeleteMessage:                {},
	MethodEditMessage:                  {},
	MethodEnsureBotDMRoom:              {},
	MethodEnsureRoomKey:                {},
	MethodGetBotRoom:                   {},
	MethodGetChatlist:                  {},
	MethodGetCurrentUser:               {},
	MethodGetDMSubscription:            {},
	MethodGetMessage:                   {},
	MethodGetRoomAppCommandMenu:        {},
	MethodGetRoomAppTabs:               {},
	MethodGetRoomKey:                   {},
	MethodGetSettings:                  {},
	MethodGetSubscriptionByRoom:        {},
	MethodGetThreadUnreadSummary:       {},
	MethodGetUserProfile:               {},
	MethodGetUserStatus:                {},
	MethodListAppCategories:            {},
	MethodListApps:                     {},
	MethodListChannelMessages:          {},
	MethodListChannelSubscriptions:     {},
	MethodListEmojis:                   {},
	MethodListMemberStatuses:           {},
	MethodListMembers:                  {},
	MethodListMentionableSubscriptions: {},
	MethodListMessageReaders:           {},
	MethodListNextMessages:             {},
	MethodListOrgMembers:               {},
	MethodListPinnedMessages:           {},
	MethodListPriorityContacts:         {},
	MethodListSubscriptions:            {},
	MethodListSurroundingMessages:      {},
	MethodListThreadMessages:           {},
	MethodListThreadParentMessages:     {},
	MethodListThreadSubscriptions:      {},
	MethodListUserThreads:              {},
	MethodMarkAllThreadsRead:           {},
	MethodMarkRoomRead:                 {},
	MethodMarkRoomThreadsRead:          {},
	MethodMarkThreadRead:               {},
	MethodMigrateDeleteMessage:         {},
	MethodMigrateEditMessage:           {},
	MethodMoveChat:                     {},
	MethodOpenRoom:                     {},
	MethodPinMessage:                   {},
	// #nosec G101 -- an rpc.method label value, not a credential: it names the route that refreshes or sets a token, and is exported to Prometheus as a metric dimension
	MethodRefreshSSOToken:            {},
	MethodRemoveBotRoomMembers:       {},
	MethodRemoveMember:               {},
	MethodRemovePriorityContact:      {},
	MethodRenameChatlistSection:      {},
	MethodRenameRoom:                 {},
	MethodReorderChatlistSections:    {},
	MethodSearchApps:                 {},
	MethodSearchMessages:             {},
	MethodSearchOrgs:                 {},
	MethodSearchRooms:                {},
	MethodSearchUsers:                {},
	MethodSendDM:                     {},
	MethodSendRoomMessage:            {},
	MethodSetAppSubscription:         {},
	MethodSetChatlistSectionSortMode: {},
	MethodSetManualPresence:          {},
	MethodSetRoomRestricted:          {},
	MethodSetSettings:                {},
	// #nosec G101 -- an rpc.method label value, not a credential: it names the route that refreshes or sets a token, and is exported to Prometheus as a metric dimension
	MethodSetSSOToken:           {},
	MethodSetUserStatus:         {},
	MethodStartTeamsRoomCall:    {},
	MethodStartTeamsUserCall:    {},
	MethodToggleFavorite:        {},
	MethodToggleMessageReaction: {},
	MethodToggleMute:            {},
	MethodTranslateText:         {},
	MethodUnpinMessage:          {},
	MethodUpdateMemberRole:      {},
}

// Valid reports whether m is one of the vocabulary constants. MethodOther is
// not, by design.
func (m RPCMethod) Valid() bool {
	_, ok := rpcMethodVocabulary[m]
	return ok
}

// rpcMethodVocabularySize exists for the completeness test; nothing in
// production needs the count.
func rpcMethodVocabularySize() int { return len(rpcMethodVocabulary) }
