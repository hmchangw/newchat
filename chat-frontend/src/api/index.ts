// Public surface of the api/ layer.
//
// Each operation hides its transport (NATS request/reply, JetStream
// publish, two-phase async-job) so callers just say `addMembers(nats,
// args)` and don't need to know what subject it lands on.
//
// Components should ONLY import from this file. The `_transport/`
// folder is internal — subjects, the two-phase request helper, and
// the wire-shape normaliser live there because they're implementation
// details of the api/ layer. Anything callers legitimately need from
// transport (the error class, the format helper, the error-kind enum)
// is re-exported here.

export { addMembers } from './addMembers'
export {
  getChatlist,
  seedChatlistDemo,
  createChatlistSection,
  renameChatlistSection,
  deleteChatlistSection,
  reorderChatlistSections,
  setChatlistSectionSortMode,
} from './chatlist'
export { moveChat } from './moveChat'
export { subscribeToChatlistUpdates } from './subscribeToChatlistUpdates'
export { createRoom } from './createRoom'
export { deleteMessage } from './deleteMessage'
export { editMessage } from './editMessage'
export { fetchMessageHistory } from './fetchMessageHistory'
export { fetchReadReceipt } from './fetchReadReceipt'
export { fetchSidebarBuckets, subToRoom, keyEntryFor, PAGE_LIMIT, MAX_PAGES } from './fetchSidebarBuckets'
export { fetchSurroundingMessages } from './fetchSurroundingMessages'
export { fetchThreadMessages } from './fetchThreadMessages'
export { getUnreadCount } from './getUnreadCount'
export { leaveRoom } from './leaveRoom'
export { listOrgMembers } from './listOrgMembers'
export { listRoomMembers } from './listRoomMembers'
export { markRoomRead } from './markRoomRead'
export { removeMember } from './removeMember'
export { requestRoomKey } from './requestRoomKey'
export { searchMessages } from './searchMessages'
export { searchRooms } from './searchRooms'
export { sendMessage } from './sendMessage'
export { setMediaCookie } from './setMediaCookie'
export { subscribeToRoomEvents } from './subscribeToRoomEvents'
export { subscribeToThreadEvents } from './subscribeToThreadEvents'
export { subscribeToRoomMetadataUpdates } from './subscribeToRoomMetadataUpdates'
export { subscribeToRoomKeyEvents } from './subscribeToRoomKeyEvents'
export { subscribeToSubscriptionUpdates } from './subscribeToSubscriptionUpdates'
export { subscribeToUserRoomEvents } from './subscribeToUserRoomEvents'
export { updateMemberRole } from './updateMemberRole'
export { uploadImage } from './uploadImage'

// Transport-level error utilities that callers legitimately need.
// `_transport/` stays internal otherwise.
export {
  AsyncJobError,
  ASYNC_JOB_ERROR_KINDS,
  formatAsyncJobError,
} from './_transport/asyncJob'
export type { AsyncJobErrorKind, ErrorCode } from './_transport/asyncJob'

// Shared wire types — mirror pkg/model. Components/contexts import
// these from `@/api` instead of deep-importing `@/api/types`.
export type {
  Nats,
  NatsSubscription,
  SubscriptionCallback,
  SubscriptionUpdateEvent,
  SubscriptionUpdateAction,
  AsyncJobOptions,
  AsyncJobResult,
  AsyncJobResultEnvelope,
  // Domain types
  User,
  Room,
  RoomType,
  Role,
  Subscription,
  DMSubscription,
  SubscriptionHRInfo,
  Message,
  HistoryMessage,
  Participant,
  PreviewMessage,
  QuotedParentMessage,
  MemberEntry,
  Reader,
  ChannelRef,
  HistoryConfig,
  HistoryMode,
  RoomKeyEvent,
  ChatlistSortMode,
  ChatlistSection,
  ChatlistState,
  ChatlistUpdateEvent,
} from './types'
