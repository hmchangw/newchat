package main

import soakwire "github.com/hmchangw/chat/tools/loadgen/internal/soak/wire"

// These aliases keep the existing soak call sites stable while the wire DTOs
// move behind an explicit package boundary. They are removed as the callers
// move into their owning packages.
type soakRoomMeta = soakwire.RoomMeta
type soakLoadHistoryRequest = soakwire.LoadHistoryRequest
type soakLoadHistoryResponse = soakwire.LoadHistoryResponse
type soakLoadNextMessagesRequest = soakwire.LoadNextMessagesRequest
type soakLoadNextMessagesResponse = soakwire.LoadNextMessagesResponse
type soakGetMessageByIDRequest = soakwire.GetMessageByIDRequest
type soakWireMessage = soakwire.Message
type soakEditMessageRequest = soakwire.EditMessageRequest
type soakEditMessageResponse = soakwire.EditMessageResponse
type soakDeleteMessageRequest = soakwire.DeleteMessageRequest
type soakDeleteMessageResponse = soakwire.DeleteMessageResponse
type soakPinMessageRequest = soakwire.PinMessageRequest
type soakPinMessageResponse = soakwire.PinMessageResponse
type soakUnpinMessageRequest = soakwire.UnpinMessageRequest
type soakUnpinMessageResponse = soakwire.UnpinMessageResponse
type soakListPinnedMessagesRequest = soakwire.ListPinnedMessagesRequest
type soakListPinnedMessagesResponse = soakwire.ListPinnedMessagesResponse
type soakReactMessageRequest = soakwire.ReactMessageRequest
type soakReactMessageResponse = soakwire.ReactMessageResponse
type soakGetThreadMessagesRequest = soakwire.GetThreadMessagesRequest
type soakGetThreadMessagesResponse = soakwire.GetThreadMessagesResponse
type soakAddMembersRequest = soakwire.AddMembersRequest
type soakRemoveMemberRequest = soakwire.RemoveMemberRequest
type soakRoomRenameRequest = soakwire.RoomRenameRequest
type soakCreateRoomRequest = soakwire.CreateRoomRequest
type soakStatusReply = soakwire.StatusReply
type soakCreateRoomReply = soakwire.CreateRoomReply
type soakMuteToggleReply = soakwire.MuteToggleReply
type soakListMembersResponse = soakwire.ListMembersResponse
type soakRoomsInfoRequest = soakwire.RoomsInfoRequest
type soakRoomInfo = soakwire.RoomInfo
type soakRoomsInfoResponse = soakwire.RoomsInfoResponse
type soakReadReceiptRequest = soakwire.ReadReceiptRequest
type soakReadReceiptResponse = soakwire.ReadReceiptResponse
type soakSubscriptionListResponse = soakwire.SubscriptionListResponse
type soakSubscriptionListRequest = soakwire.SubscriptionListRequest
type soakUserNameRequest = soakwire.UserNameRequest
type soakUserAccountNameRequest = soakwire.UserAccountNameRequest
type soakUserRoomRequest = soakwire.UserRoomRequest
type soakUserPageRequest = soakwire.UserPageRequest
type soakUserChannelsRequest = soakwire.UserChannelsRequest
type soakUserCountRequest = soakwire.UserCountRequest
type soakUserEmptyRequest = soakwire.UserEmptyRequest
type soakUserMeResponse = soakwire.UserMeResponse
type soakUserStatusResponse = soakwire.UserStatusResponse
type soakUserSettingsResponse = soakwire.UserSettingsResponse
type soakUserChatlistResponse = soakwire.UserChatlistResponse
type soakUserPriorityContactsResponse = soakwire.UserPriorityContactsResponse
type soakUserAppsResponse = soakwire.UserAppsResponse
type soakUserAppCategoriesResponse = soakwire.UserAppCategoriesResponse
type soakUserCountResponse = soakwire.UserCountResponse
type soakUserDMResponse = soakwire.UserDMResponse
type soakUserThreadListResponse = soakwire.UserThreadListResponse
type soakUserThreadUnreadResponse = soakwire.UserThreadUnreadResponse

const soakSubscriptionListType = soakwire.SubscriptionListType
