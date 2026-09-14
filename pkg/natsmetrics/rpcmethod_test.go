package natsmetrics

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// allRPCMethods enumerates the vocabulary so the tests below can walk it. It
// mirrors enums_test.go's allPublishOperations: Go has no way to range over a
// const block, and a hand-kept list that drifts is caught by
// TestRPCMethodVocabularyIsComplete, which cross-checks it against Valid().
var allRPCMethods = []RPCMethod{
	MethodAddBotRoomMembers,
	MethodAddMembers,
	MethodAddPriorityContact,
	MethodBatchGetBadgeCounts,
	MethodBatchGetMessages,
	MethodBatchGetPeerPresence,
	MethodBatchGetPresence,
	MethodBatchGetRoomPreviews,
	MethodBatchGetRoomsInfo,
	MethodBatchGetThreadRoomsInfo,
	MethodCountSubscriptions,
	MethodCreateBotRoom,
	MethodCreateChatlistSection,
	MethodCreateDMRoom,
	MethodCreateRoom,
	MethodCreateTeamsMeeting,
	MethodDeleteChatlistSection,
	MethodDeleteEmoji,
	MethodDeleteMessage,
	MethodEditMessage,
	MethodEnsureBotDMRoom,
	MethodEnsureRoomKey,
	MethodGetBotRoom,
	MethodGetChatlist,
	MethodGetCurrentUser,
	MethodGetDMSubscription,
	MethodGetMessage,
	MethodGetRoomAppCommandMenu,
	MethodGetRoomAppTabs,
	MethodGetRoomKey,
	MethodGetSettings,
	MethodGetSubscriptionByRoom,
	MethodGetThreadUnreadSummary,
	MethodGetUserProfile,
	MethodGetUserStatus,
	MethodListAppCategories,
	MethodListApps,
	MethodListChannelMessages,
	MethodListChannelSubscriptions,
	MethodListEmojis,
	MethodListMemberStatuses,
	MethodListMembers,
	MethodListMentionableSubscriptions,
	MethodListMessageReaders,
	MethodListNextMessages,
	MethodListOrgMembers,
	MethodListPinnedMessages,
	MethodListPriorityContacts,
	MethodListSubscriptions,
	MethodListSurroundingMessages,
	MethodListThreadMessages,
	MethodListThreadParentMessages,
	MethodListThreadSubscriptions,
	MethodListUserThreads,
	MethodMarkAllThreadsRead,
	MethodMarkRoomRead,
	MethodMarkSiteThreadsRead,
	MethodMarkThreadRead,
	MethodMigrateDeleteMessage,
	MethodMigrateEditMessage,
	MethodMoveChat,
	MethodOpenRoom,
	MethodPinMessage,
	MethodRefreshSSOToken,
	MethodRemoveBotRoomMembers,
	MethodRemoveMember,
	MethodRemovePriorityContact,
	MethodRenameChatlistSection,
	MethodRenameRoom,
	MethodReorderChatlistSections,
	MethodSearchApps,
	MethodSearchMessages,
	MethodSearchOrgs,
	MethodSearchRooms,
	MethodSearchUsers,
	MethodSendDM,
	MethodSendRoomMessage,
	MethodSetAppSubscription,
	MethodSetChatlistSectionSortMode,
	MethodSetManualPresence,
	MethodSetRoomRestricted,
	MethodSetSettings,
	MethodSetSSOToken,
	MethodSetUserStatus,
	MethodStartTeamsRoomCall,
	MethodStartTeamsUserCall,
	MethodToggleFavorite,
	MethodToggleMessageReaction,
	MethodToggleMute,
	MethodTranslateText,
	MethodUnpinMessage,
	MethodUpdateMemberRole,
}

// TestEveryRPCMethodIsValid pins the vocabulary as the closed set Valid()
// reports. A constant that fails here is declared but unusable: addRPCRoute
// degrades an invalid method to MethodOther, so the route would silently
// record under the fallback instead of its own name.
func TestEveryRPCMethodIsValid(t *testing.T) {
	for _, m := range allRPCMethods {
		assert.True(t, m.Valid(), "method %q is declared but not Valid()", m)
	}
}

// TestMethodOtherIsNotValid keeps the fallback outside the vocabulary. _OTHER
// is what registration degrades to when a caller passes something unusable;
// if it were Valid() a route could claim it deliberately and the fallback
// would stop meaning "this should never happen".
func TestMethodOtherIsNotValid(t *testing.T) {
	assert.False(t, MethodOther.Valid(),
		"MethodOther must stay outside the vocabulary so no route can claim it")
}

// TestRPCMethodValuesAreUniqueAndWellFormed guards the label itself: a
// duplicate value silently merges two routes into one time series, and a value
// outside snake_case breaks the naming rule the vocabulary documents.
func TestRPCMethodValuesAreUniqueAndWellFormed(t *testing.T) {
	snakeCase := regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)
	seen := make(map[RPCMethod]struct{}, len(allRPCMethods))

	for _, m := range allRPCMethods {
		_, dup := seen[m]
		require.False(t, dup, "duplicate RPCMethod value %q", m)
		seen[m] = struct{}{}
		assert.Regexp(t, snakeCase, string(m), "method %q is not verb-first snake_case", m)
	}
}

// TestRPCMethodVocabularyIsComplete catches the list above drifting from the
// const block. Valid() is driven by its own table, so a constant added to one
// and not the other shows up as a count mismatch here rather than as a route
// silently recording under MethodOther in production.
func TestRPCMethodVocabularyIsComplete(t *testing.T) {
	assert.Equal(t, len(allRPCMethods), rpcMethodVocabularySize(),
		"allRPCMethods and the Valid() table disagree; add the constant to both")
}
