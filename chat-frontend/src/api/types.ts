// Shared types for the api/ layer.
//
// These mirror the Go server's pkg/model. Keep field names + casing in
// sync with the JSON tags on those structs — the frontend talks to the
// server in JSON, so what's on the wire IS the contract.
//
// Anything that's specific to a single operation lives in that
// operation's index.ts (e.g. its `Args`/`Response` types). Cross-cutting
// shapes live here.

/** Mirrors model.RoomType. */
export type RoomType = 'channel' | 'dm' | 'botDM' | 'discussion'

/** Mirrors model.Role. */
export type Role = 'owner' | 'admin' | 'member'

/** Mirrors pkg/model.SubscriptionHRInfo — the counterpart's HR record
 *  attached to a DM subscription. All three fields are required strings
 *  on the wire (no `omitempty` server-side). */
export interface SubscriptionHRInfo {
  account: string
  name: string
  engName: string
}

/** Room-level fields nested under `Subscription.room` after enrichment.
 *  `name` here is the room's CANONICAL name — the subscription's own
 *  `name` (counterpart account for DMs, app display name for botDMs,
 *  channel name for channels) is never overwritten by it. */
export interface SubscriptionRoom {
  siteId?: string
  name?: string
  userCount?: number
  appCount?: number
  lastMsgAt?: string | null
  lastMsgId?: string
  lastMentionAllAt?: string | null
  /** Base64-encoded room E2E private key — delivered on subscription.list
   *  for initial key bootstrap (same payload as the room.key.get RPC). */
  privateKey?: string
  keyVersion?: number
}

/** Mirrors pkg/model.Subscription — the per-user record linking a user
 *  to a room (channel / botDM / discussion). Carries the room's roles
 *  for THIS user, the user's preferred name, mute/alert state, and
 *  mention/thread-unread bookkeeping. DM subscriptions are a separate
 *  type (`DMSubscription`) that extends this with HRInfo. */
export interface Subscription {
  id: string
  u: { id: string; account: string; isBot?: boolean }
  roomId: string
  siteId: string
  roles: Role[]
  name: string
  roomType: RoomType
  isSubscribed?: boolean
  historySharedSince?: string
  joinedAt: string
  lastSeenAt?: string
  hasMention: boolean
  threadUnread?: string[]
  alert: boolean
  /** Room-level metadata nested under `room` after enrichment by
   *  user-service. Present on `subscription.list` replies; absent on
   *  live `subscription.update` events. Default to 0 / null at the
   *  consumer. */
  room?: SubscriptionRoom
}

/**
 * Mirrors pkg/model.DMSubscription — Go embeds `*Subscription` and adds
 * `HRInfo *SubscriptionHRInfo \`json:"hrInfo,omitempty"\``. Embedded-struct
 * JSON serialization flattens: on the wire a DMSubscription is a
 * Subscription with one extra top-level `hrInfo` field. Backend emits
 * this wrapper only for `roomType === 'dm'` subscriptions; channels /
 * botDMs / discussions ship plain Subscription (no hrInfo).
 *
 * For consumer ergonomics the reducer's state map + api wire types use
 * `DMSubscription` for every entry — channels just have `hrInfo`
 * undefined. Components needing hrInfo (e.g. `roomFormat`'s DM
 * display) read `sub.hrInfo` directly with no narrow.
 */
export interface DMSubscription extends Subscription {
  hrInfo?: SubscriptionHRInfo
}

/** Mirrors model.HistoryMode. */
export type HistoryMode = 'all' | 'none'

/** Mirrors model.HistoryConfig. */
export interface HistoryConfig {
  mode: HistoryMode
}

/** Mirrors model.ChannelRef — the {roomId, siteId} pair used for cross-
 *  site channel-source references in member.add / room.create payloads. */
export interface ChannelRef {
  roomId: string
  siteId: string
}

/** Mirrors model.User as it ships down from auth-service (after the
 *  NATS handshake). siteId is added client-side in NatsContext. The
 *  other sect/employee fields arrive from the auth payload but might
 *  be empty depending on the user's directory entry. */
export interface User {
  id: string
  account: string
  siteId: string
  sectId?: string
  sectName?: string
  engName?: string
  chineseName?: string
  employeeId?: string
}

/** Mirrors model.Room. Timestamps come down as RFC-3339 strings.
 *  `lastMsgAt`, `lastMentionAllAt`, `minUserLastSeenAt` are `omitempty`
 *  on the wire — typed optional. The rest are always present. */
export interface Room {
  id: string
  name: string
  type: RoomType
  siteId: string
  userCount: number
  appCount: number
  lastMsgId: string
  lastMsgAt?: string
  lastMentionAllAt?: string
  minUserLastSeenAt?: string
  createdAt: string
  updatedAt: string
  restricted?: boolean
  /** Set client-side on DM rooms so the sidebar has a friendly fallback
   *  while the canonical name lands via subscription.update. */
  subscriptionName?: string
}

/** Mirrors model.Participant — the embedded sender/reader on messages
 *  and read-receipts. `siteId` rides along on enriched senders. */
export interface Participant {
  account: string
  userId?: string
  engName?: string
  chineseName?: string
  siteId?: string
}

/** Cassandra's QuotedParentMessage shape — what gets embedded on a
 *  reply's `quotedParentMessage` field. Note the legacy `messageId`
 *  and `msg` field names (server-side cassandra schema, distinct from
 *  the model.Message wire shape that uses `id` + `content`). */
export interface QuotedParentMessage {
  messageId: string
  sender?: Participant
  msg?: string
}

/**
 * Cassandra-shape message as it arrives from history-service.
 * Distinct from the broadcast `Message` shape: history rows carry
 * `messageId` + `msg`, broadcasts carry `id` + `content`. The api/
 * layer normalises history results into `Message` before handing them
 * to callers — this type is mostly internal to the normalisation step.
 */
export interface HistoryMessage {
  messageId: string
  roomId: string
  sender?: Participant
  createdAt: string
  msg: string
  editedAt?: string
  deleted?: boolean
  type?: string
  sysMsgData?: string
  mentions?: Participant[]
  quotedParentMessage?: QuotedParentMessage
  threadParentId?: string
  threadParentCreatedAt?: string
  tcount?: number
}

/**
 * Normalised message shape consumed by every renderer (MessageRow,
 * QuotedBlock, SystemMessage). Broadcast events arrive in this shape;
 * historic rows are mapped to it by `normalizeHistoricalMessages` in
 * the api layer.
 */
export interface Message {
  id: string
  content: string
  createdAt: string
  editedAt?: string
  deleted?: boolean
  type?: string
  sysMsgData?: string
  sender?: Participant
  userAccount?: string
  mentions?: Participant[]
  quotedParentMessage?: QuotedParentMessage
  threadParentMessageId?: string
  threadParentMessageCreatedAt?: string
  /** Outgoing local-only flag — set by optimistic appenders. Never
   *  arrives from the server. */
  _local?: boolean
  /** Client-side delivery state. Set to 'failed' when an optimistic
   *  message couldn't be published. */
  _status?: 'failed'
  /** Thread reply count, surfaced as the "💬 N replies" badge. */
  tcount?: number
}

/** Mirrors model.RoomMember. `id` is the Mongo doc id, `rid` is the
 *  containing room, `ts` is the join timestamp. `member` carries the
 *  type-tagged details (individual vs org). */
export interface MemberEntry {
  id: string
  rid: string
  ts: string
  member: {
    type: 'individual' | 'org'
    id: string
    account?: string
    engName?: string
    chineseName?: string
    sectName?: string
    memberCount?: number
    isOwner?: boolean
  }
}

/** Reader entry returned by the read-receipt RPC. */
export interface Reader {
  userId: string
  account: string
  engName?: string
  chineseName?: string
}

/** Handle returned by the NATS subscribe primitive — the client gives
 *  us an object with `.unsubscribe()`. Narrowed here so callers don't
 *  need to import nats.ws's full type.
 *
 *  NOTE: distinct from the `Subscription` model type above (which is
 *  the per-user-per-room record from `pkg/model.Subscription`).
 *  Same name would cause TS declaration merging to silently fuse the
 *  two interfaces — disasterously, since callers would then see
 *  `.roles` typed on a NATS handle. Renamed to `NatsSubscription` to
 *  keep the two contracts strictly separate. */
export interface NatsSubscription {
  unsubscribe: () => void
}

/** Inbound NATS event payload. Subscribers narrow on `evt.type` to
 *  reach the variant fields. Typed `unknown` so call sites can't
 *  bypass the discriminator. */
export type SubscriptionCallback = (event: unknown) => void

/** Known values of `SubscriptionUpdateEvent.action`. Backend (room-worker
 *  `handler.go`) emits `"added"` on member-add / room-create / DM-sync,
 *  `"removed"` on member-remove, and `"role_updated"` on role change.
 *  Typed as a union ∪ `string` so consumers get autocomplete for the
 *  known values but forward-compat with any new action the backend
 *  introduces. */
export type SubscriptionUpdateAction = 'added' | 'removed' | 'role_updated' | (string & {})

/** Wire shape of `chat.user.{account}.event.subscription.update` events.
 *  Mirrors `pkg/model.SubscriptionUpdateEvent`. Subscription is typed
 *  as the DM variant so callers can read `hrInfo` without narrowing
 *  (channels/groups omit the field; DMs carry it). */
export interface SubscriptionUpdateEvent {
  userId: string
  subscription: DMSubscription
  action: SubscriptionUpdateAction
  timestamp: number
}

/**
 * Mirrors pkg/model.RoomKeyEvent — payload of
 * chat.user.{account}.event.room.key. PrivateKey is base64-encoded on
 * the wire (Go's encoding/json default for []byte). PublicKey is
 * omitted from the client wire payload.
 */
export interface RoomKeyEvent {
  roomId: string
  version: number
  privateKey: string  // base64
  timestamp: number
}

/** Two-phase async-job result returned by `requestWithAsyncResult`. The
 *  outer wrapper; `S` is the sync reply shape, `A` is the wire-side async
 *  payload (typically `AsyncJobResultEnvelope` below). */
export interface AsyncJobResult<S = unknown, A = unknown> {
  requestId: string
  sync: S
  async: A | null
}

/**
 * Wire-side envelope published by room-worker on the per-request response
 * subject when an async job finishes. Mirrors `pkg/model.AsyncJobResult`
 * field-by-field (the strict TS-mirror rule). Use as the `A` generic on
 * `AsyncJobResult<S, A>` when the caller wants typed access to the worker's
 * result payload.
 *
 * `code` and `reason` mirror the errcode envelope and are populated only
 * when `status === 'error'`. Branch on `reason ?? code`; never on `error`
 * text. See chat-frontend/CLAUDE.md "Error envelope (server contract)".
 */
export interface AsyncJobResultEnvelope {
  requestId: string
  operation: string
  status: 'ok' | 'error'
  roomId?: string
  error?: string
  code?: string
  reason?: string
  timestamp: number
}

/** Debug verbosity level driven by the UI selector. 'off' sends no header;
 *  the others are sent verbatim as the `X-Debug` header value. */
export type DebugLevel = 'off' | 'flow' | 'debug' | 'trace'

/** Options forwarded to `requestWithAsyncResult` from the api layer. */
export interface AsyncJobOptions {
  /** When set, a sync reply matching this predicate is treated as
   *  success (e.g. the DM-exists dedup reply on room.create). */
  treatAsSuccess?: (reply: unknown) => boolean
  /** Override the auto-generated request ID. */
  requestId?: string
  syncTimeout?: number
  asyncTimeout?: number
  /** When set to a non-'off' level, stamp an `X-Debug: <level>` header on the
   *  request so the backend scales diagnostics. Driven by the UI selector. */
  debugLevel?: DebugLevel
  /** When true, stamp an `X-Debug-Payload: 1` header so the backend captures
   *  full request/reply payloads (only honored where DEBUG_LOG_PAYLOADS is on).
   *  Independent of `debugLevel`. */
  debugPayload?: boolean
}

/**
 * The shape of `useNats()`'s return value as consumed by the api layer.
 *
 * `request<T>` is generic so each api op declares its response shape
 * once and gets type-checked at the call site — `Promise<any>` would
 * silently swallow shape mismatches.
 *
 * Only the fields the api layer actually uses are typed; NatsContext
 * also exposes `connect`/`disconnect`/`connected`/`error`, but those
 * are component-facing and JSX consumers don't need a TS type yet.
 */
export interface Nats {
  user: User
  request: <T = unknown>(subject: string, data?: unknown) => Promise<T>
  publish: (subject: string, data?: unknown) => void
  subscribe: (subject: string, cb: SubscriptionCallback) => NatsSubscription
  requestWithAsyncResult: <S = unknown, A = unknown>(
    subject: string,
    data?: unknown,
    opts?: AsyncJobOptions,
  ) => Promise<AsyncJobResult<S, A>>
}
