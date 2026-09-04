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
