package natsmetrics

// RPCMethod is a closed, code-owned label for the rpc_method metric
// dimension. A route declares its method at registration time; the value
// never comes from a subject, a client payload, or any other dynamic source.
type RPCMethod string

const (
	// room-service
	MethodToggleMute                   RPCMethod = "toggle_mute"
	MethodToggleFavorite               RPCMethod = "toggle_favorite"
	MethodMoveChat                     RPCMethod = "move_chat"
	MethodOpenRoom                     RPCMethod = "open_room"
	MethodGetRoomAppTabs               RPCMethod = "get_room_app_tabs"
	MethodGetRoomAppCommandMenu        RPCMethod = "get_room_app_command_menu"
	MethodListOrgMembers               RPCMethod = "list_org_members"
	MethodListMembers                  RPCMethod = "list_members"
	MethodListMemberStatuses           RPCMethod = "list_member_statuses"
	MethodListMentionableSubscriptions RPCMethod = "list_mentionable_subscriptions"
	MethodGetRoomKey                   RPCMethod = "get_room_key"
	MethodMarkMessageRead              RPCMethod = "mark_message_read"
	MethodListMessageReaders           RPCMethod = "list_message_readers"
	MethodMarkThreadRead               RPCMethod = "mark_thread_read"
	MethodUpdateMemberRole             RPCMethod = "update_member_role"
	MethodRemoveMember                 RPCMethod = "remove_member"
	MethodAddMembers                   RPCMethod = "add_members"
	MethodRenameRoom                   RPCMethod = "rename_room"
	MethodSetRoomRestricted            RPCMethod = "set_room_restricted"
	MethodBatchGetRoomsInfo            RPCMethod = "batch_get_rooms_info"
	MethodBatchGetThreadRoomsInfo      RPCMethod = "batch_get_thread_rooms_info"
	MethodMarkAllThreadsRead           RPCMethod = "mark_all_threads_read"
	MethodEnsureRoomKey                RPCMethod = "ensure_room_key"
	MethodCreateRoom                   RPCMethod = "create_room"
	MethodStartTeamsRoomCall           RPCMethod = "start_teams_room_call"
	MethodStartTeamsUserCall           RPCMethod = "start_teams_user_call"
	MethodCreateTeamsMeeting           RPCMethod = "create_teams_meeting"

	// history-service
	MethodGetChannelHistory       RPCMethod = "get_channel_history"
	MethodGetThreadMessages       RPCMethod = "get_thread_messages"
	MethodGetNextMessages         RPCMethod = "get_next_messages"
	MethodGetSurroundingMessages  RPCMethod = "get_surrounding_messages"
	MethodGetMessage              RPCMethod = "get_message"
	MethodBatchGetMessages        RPCMethod = "batch_get_messages"
	MethodBatchGetRooms           RPCMethod = "batch_get_rooms"
	MethodListPinnedMessages      RPCMethod = "list_pinned_messages"
	MethodGetThreadParentMessages RPCMethod = "get_thread_parent_messages"
	MethodListThreadSubscriptions RPCMethod = "list_thread_subscriptions"
	MethodEditMessage             RPCMethod = "edit_message"
	MethodDeleteMessage           RPCMethod = "delete_message"
	MethodPinMessage              RPCMethod = "pin_message"
	MethodUnpinMessage            RPCMethod = "unpin_message"
	MethodToggleMessageReaction   RPCMethod = "toggle_message_reaction"
	MethodMigrateEditMessage      RPCMethod = "migrate_edit_message"
	MethodMigrateDeleteMessage    RPCMethod = "migrate_delete_message"

	// user-service (MethodMarkAllThreadsRead is shared with room-service)
	MethodGetCurrentUser             RPCMethod = "get_current_user"
	MethodGetUserStatus              RPCMethod = "get_user_status"
	MethodGetUserProfile             RPCMethod = "get_user_profile"
	MethodSetUserStatus              RPCMethod = "set_user_status"
	MethodGetSettings                RPCMethod = "get_settings"
	MethodSetSettings                RPCMethod = "set_settings"
	MethodListPriorityContacts       RPCMethod = "list_priority_contacts"
	MethodAddPriorityContact         RPCMethod = "add_priority_contact"
	MethodRemovePriorityContact      RPCMethod = "remove_priority_contact"
	MethodGetChatlist                RPCMethod = "get_chatlist"
	MethodCreateChatlistSection      RPCMethod = "create_chatlist_section"
	MethodDeleteChatlistSection      RPCMethod = "delete_chatlist_section"
	MethodRenameChatlistSection      RPCMethod = "rename_chatlist_section"
	MethodReorderChatlistSections    RPCMethod = "reorder_chatlist_sections"
	MethodSetChatlistSectionSortMode RPCMethod = "set_chatlist_section_sort_mode"
	MethodListSubscriptions          RPCMethod = "list_subscriptions"
	MethodListUserThreads            RPCMethod = "list_user_threads"
	MethodGetThreadUnreadSummary     RPCMethod = "get_thread_unread_summary"
	MethodListChannelSubscriptions   RPCMethod = "list_channel_subscriptions"
	MethodGetDMSubscription          RPCMethod = "get_dm_subscription"
	MethodGetSubscriptionByRoom      RPCMethod = "get_subscription_by_room"
	MethodCountSubscriptions         RPCMethod = "count_subscriptions"
	MethodSetAppSubscription         RPCMethod = "set_app_subscription"
	MethodListApps                   RPCMethod = "list_apps"
	MethodListAppCategories          RPCMethod = "list_app_categories"
	// #nosec G101 -- metric label name, not a credential
	MethodSetSSOToken RPCMethod = "set_sso_token"
	// #nosec G101 -- metric label name, not a credential
	MethodRefreshSSOToken     RPCMethod = "refresh_sso_token"
	MethodBatchGetBadgeCounts RPCMethod = "batch_get_badge_counts"

	// search-service
	MethodSearchMessages RPCMethod = "search_messages"
	MethodSearchRooms    RPCMethod = "search_rooms"
	MethodSearchApps     RPCMethod = "search_apps"
	MethodSearchUsers    RPCMethod = "search_users"
	MethodSearchOrgs     RPCMethod = "search_orgs"

	// media-service / translation-service / room-worker
	MethodListEmojis    RPCMethod = "list_emojis"
	MethodDeleteEmoji   RPCMethod = "delete_emoji"
	MethodTranslateText RPCMethod = "translate_text"
	MethodCreateDMRoom  RPCMethod = "create_dm_room"

	// user-presence-service
	MethodSetManualPresence    RPCMethod = "set_manual_presence"
	MethodBatchGetPresence     RPCMethod = "batch_get_presence"
	MethodBatchGetPeerPresence RPCMethod = "batch_get_peer_presence"

	// bot lane
	MethodCreateBotRoom        RPCMethod = "create_bot_room"
	MethodAddBotRoomMembers    RPCMethod = "add_bot_room_members"
	MethodRemoveBotRoomMembers RPCMethod = "remove_bot_room_members"
	MethodGetBotRoom           RPCMethod = "get_bot_room"
	MethodEnsureBotDMRoom      RPCMethod = "ensure_bot_dm_room"
	MethodSendRoomMessage      RPCMethod = "send_room_message"
	MethodSendDM               RPCMethod = "send_dm"

	// client-only: chat.presence.{site}.request.snapshot has no subscriber in
	// this repo and is gated off by PRESENCE_RPC_ENABLED=false.
	MethodGetPresenceSnapshot RPCMethod = "get_presence_snapshot"

	// MethodOther is the value recorded for a method outside the vocabulary.
	// semconv makes this normative for rpc.method: "When the method is not
	// recognized … the attribute MUST be set to `_OTHER`"
	// (semconv/v1.40.0/attribute_group.go:13902).
	//
	// In steady state it must not appear at all. Registration is gated by a
	// semgrep rule and a per-service registration test, so a value reaching here
	// means both were bypassed — alert on it rather than treating it as a bucket.
	MethodOther RPCMethod = "_OTHER"
)

// rpcMethods is the vocabulary. It is the ONLY list: Valid and
// normalizeRPCMethod read it, and the naming guards in rpcmethod_test.go
// iterate it. A method declared above but missing here is not registerable,
// which is a loud failure; the previous shape — a switch here and a separate
// copy in the test file — made the opposite mistake silent.
var rpcMethods = []RPCMethod{
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

var rpcMethodSet = func() map[RPCMethod]struct{} {
	set := make(map[RPCMethod]struct{}, len(rpcMethods))
	for _, m := range rpcMethods {
		set[m] = struct{}{}
	}
	return set
}()

// normalizeRPCMethod bounds the label at record time.
func normalizeRPCMethod(m RPCMethod) RPCMethod {
	if m.Valid() {
		return m
	}
	return MethodOther
}

// Valid reports whether m is a declared method, and is what natsrouter checks
// before it will register a route. Exported for that reason and no other: a
// caller that can build an RPCMethod from an arbitrary string is exactly the
// hole registration is meant to close, and the metric label must stay bounded
// whichever way the value arrives.
//
// MethodOther is deliberately not valid. It is the record-time fallback for a
// value that should never occur, not a method a route may claim.
func (m RPCMethod) Valid() bool {
	_, ok := rpcMethodSet[m]
	return ok
}
