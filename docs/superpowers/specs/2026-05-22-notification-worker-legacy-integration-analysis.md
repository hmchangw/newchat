# Notification Worker: Legacy System Integration Analysis

**Date:** 2026-05-22  
**Status:** Research Summary for Design Doc Completion  
**Purpose:** This document analyzes patterns from a legacy notification system implementation to inform the design of the new notification-worker service.

---

## 1. Executive Summary

The current notification-worker spec identifies three key gaps:
1. **Cache optimization** via `roomsubcache`
2. **Mobile push notifications** hand-off
3. **Bug fixes** for mute/restricted room handling

This analysis examines a **legacy enterprise chat system** to extract implementation patterns for:
- Push notification service interface
- User presence/status checking mechanism
- Hook handler behavior
- Desktop vs mobile routing logic

### Key Finding
The legacy system's hook-handler service contains **most of the logic** needed for the new notification-worker, including:
- ✅ Mobile push notifications
- ✅ Desktop horizontal broadcast notifications
- ✅ Presence-based notification filtering
- ✅ Thread message handling
- ✅ Mention detection and routing
- ✅ Restricted room filtering (`HistorySharedSince`)
- ✅ User preferences (mute, disable notifications)

**However**, the legacy architecture is **radically different** from the new backend:
- **Legacy**: Hook-handler processes messages after persistence, handles all notification logic inline
- **New backend**: notification-worker is a **standalone JetStream consumer** fanning out from `MESSAGES_CANONICAL`

---

## 2. Legacy System Implementation Patterns

### 2.1 Hook-Handler Architecture

The legacy hook-handler is a standalone service that:
1. Listens to JetStream for persisted messages
2. Registers multiple **after-save handlers**
3. One handler (`sendNotifications`) handles all notification logic

```go
// Simplified representation
func (p *Processor) registerAfterSaveMessageHandlers() {
    p.afterSaveMessageHandlers = []AfterSaveMessageHandler{
        p.updateSubscriptions,      // Updates subscription documents
        p.handleThreadMessages,     // Handles thread reply logic
        p.sendNotifications,        // <<<< NOTIFICATION LOGIC HERE
        p.updateRoomToNotifyUsers,  // Updates room metadata
        p.publishBotPlatformEvent,  // Bot platform integration
        p.publishFederationEvent,   // Cross-domain federation
    }
}
```

### 2.2 Notification Flow (sendNotifications)

**Step-by-step breakdown:**

1. **Fetch context data:**
   - Get message sender
   - Parse message content (emoji conversion, mention extraction)
   - Count room members
   - Calculate `disableAllMessageNotifications` if room exceeds threshold
   - Get thread reply user IDs if applicable

2. **Query recipients**:
   - Returns: `[](*Subscription, *User)` pairs
   - The MongoDB query applies complex notification filtering:
     - Filters based on `DisableNotification`, `Muted`
     - Handles mention logic (@all, @here, specific mentions)
     - Application-level filtering for restricted rooms

3. **Per-recipient processing loop**:

   **Thread message filtering:**
   ```go
   if subscription.HistorySharedSince != nil && 
      message.ThreadMessageID != "" && 
      !message.AlsoSendToChannel {
       
       parentMessage, err := p.messages.GetParentByID(ctx, message.ThreadMessageID)
       if time.Time(*subscription.HistorySharedSince).After(
           time.Time(parentMessage.Timestamp)) {
           continue  // Skip: user joined after thread parent
       }
   }
   ```
   ⚠️ **NOTE:** This only filters thread messages, not regular messages in restricted rooms!

   **Calculate notification parameters:**
   - `isHighlighted`: message contains user-specific highlight words
   - `hasMentionToUser`: user ID is in mention list
   - `hasReplyToThread`: user is thread reply participant

   **Build notification.Params**:
   ```go
   params := &notification.Params{
       Subscription: subscription,
       Sender: sender,
       Receiver: receiver,
       HasMentionToAll: hasMentionToAll,
       HasMentionToHere: hasMentionToHere,
       Message: message,
       Room: room,
       DisableAllMessageNotifications: disableAllMessageNotifications,
       IsHighlighted: isHighlighted,
       HasMentionToUser: hasMentionToUser,
       HasReplyToThread: hasReplyToThread,
   }
   ```

4. **Send Desktop Notification** via horizontal broadcast:
   
   **Two code paths** based on configuration:
   
   **Path A: Hook Notification (enabled)**
   ```go
   targets := p.hookNotificationValidator.AvailableTargets(params)
   if len(targets) == 0 {
       return nil  // Silent skip
   }
   return p.horizontalBroadcast.Publish(ctx, hookHorizontalBroadcastEvent{
       Type: "HookNotification",
       Payload: hookHorizontalNotificationPayload{
           Subscription: params.Subscription,
           Sender: params.Sender,
           NotifyTargets: targets,  // ["audio", "desktop", etc]
       },
   })
   ```
   
   **Path B: Standard Notification**
   ```go
   return p.horizontalBroadcast.Publish(ctx, horizontalBroadcastEvent{
       Type: "Notification",
       Payload: horizontalNotificationPayload{
           Subscription: params.Subscription,
           Sender: params.Sender,
           // ... etc
       },
   })
   ```

5. **Send Mobile Push Notification:**
   ```go
   if !p.cfg.PushNotificationEnable {
       continue  // Skip if push disabled globally
   }
   
   // Apply filtering for push (more restrictive than desktop)
   toUser := slices.Contains(mentionIDs, subscription.User.ID)
   if !room.IsDirectMessage() && !toUser && !hasMentionToAll && 
      disableAllMessageNotifications {
       continue  // Skip push for normal messages in large rooms
   }
   if IsBotUsername(subscription.User.Username) {
       continue  // Don't push to bots
   }
   
   if err := p.sendMobileNotification(ctx, room, message, receiver); err != nil {
       // Log error
   }
   ```

### 2.3 Hook Notification Validator (Presence + Preferences)

This is the **critical piece** for presence-based routing.

```go
type HookNotificationValidator struct {
    defaultUserPreferenceConfig *DefaultUserPreferenceConfig
}

type Params struct {
    Subscription                   *Subscription
    Sender                         *User
    Receiver                       *User  // Has StatusConnection field
    HasMentionToAll                bool
    HasMentionToHere               bool
    Message                        *Message
    Room                           *Room
    DisableAllMessageNotifications bool
    IsHighlighted                  bool
    HasMentionToUser               bool
    HasReplyToThread               bool
}
```

**Key Methods:**

**`AvailableTargets(params) []string`** - Returns which notification channels to use:
```go
func (hn *HookNotificationValidator) AvailableTargets(params *Params) []string {
    targets := []string{}
    if !hn.shouldNotify(params) {
        return targets
    }
    if hn.shouldNotifyAudio(params) {
        targets = append(targets, "audio")
    }
    if hn.shouldNotifyDesktop(params) {
        targets = append(targets, "desktop")
    }
    return targets
}
```

**`shouldNotifyAudio(params) bool`** - Presence-aware audio notification check:
```go
if params.Receiver.StatusConnection == "offline" ||  // User offline
   params.Receiver.Status == "busy" ||               // User busy
   params.Subscription.AudioNotifications == "nothing" {  // User disabled audio
   return false
}

// Notify for:
// - DM rooms (room.Type == "d")
// - Mentions of @all or @here
// - Highlighted messages
// - Direct mentions
result := params.Room.Type == "d" ||
    (!params.DisableAllMessageNotifications && (params.HasMentionToAll || params.HasMentionToHere)) ||
    params.IsHighlighted ||
    params.HasMentionToUser
```

**`shouldNotifyDesktop(params) bool`** - Desktop notification logic:
```go
if params.Receiver.StatusConnection == "offline" ||
   params.Receiver.Status == "busy" ||
   params.Subscription.DesktopNotifications == "nothing" {
   return false
}

// Uses default preferences if user hasn't set preference:
if params.Subscription.DesktopNotifications == "" {
    if defaultConfig.DesktopNotifications == "all" {
        return true
    }
    if defaultConfig.DesktopNotifications == "nothing" {
        return false
    }
}

result := params.Room.Type == "d" ||
    (!params.DisableAllMessageNotifications && (params.HasMentionToAll || params.HasMentionToHere)) ||
    params.IsHighlighted ||
    params.Subscription.DesktopNotifications == "all" ||
    params.HasMentionToUser
```

**Key Insight:**
- `StatusConnection` = user's connection state ("online", "offline", etc)
- `Status` = user's manual status ("busy", "away", etc)
- From User model: `type User struct { Status string; StatusConnection string }`

### 2.4 Mobile Push Notification Service

**Publish Function:**
```go
func (p *Processor) sendMobileNotification(ctx context.Context, 
    room *Room, message *Message, receiver *User) error {
    
    username := receiver.AccountName()
    fileName, fileType := message.GetFileInfo()
    
    title := room.FullName
    if title == "" {
        title = message.User.Username  // For DMs, use sender name
    }
    
    msg := replaceMentionedUsernamesWithFullNames(message.Message, message.Mentions)
    
    return p.pushNotification.Publish(ctx, pushNotificationEvent{
        ID:       fmt.Sprintf("%s-%s", message.ID, username),  // Unique ID
        RoomID:   room.ID,
        Username: username,  // TARGET DEVICE IDENTIFIER
        Title:    title,
        Body:     msg[:min(p.cfg.PushNotificationMaxMsgLength, len(msg))],
        Data: pushNotificationData{
            RoomID:            room.ID,
            MessageID:         message.ID,
            Type:              room.Type,  // "c" (channel), "d" (DM), "p" (private)
            Sender: &model.Participant{
                Account:     message.User.AccountName(),
                ChineseName: message.User.ChineseName,
                EngName:     message.User.EngName,
            },
            ThreadMessageID:   message.ThreadMessageID,
            FileName:          fileName,
            FileType:          fileType,
            ParentRoomID:      room.ParentRoomID,
            PushTime:          time.Now().Format(time.RFC3339),
            AlsoSendToChannel: message.AlsoSendToChannel,
        },
    })
}
```

**Event Schema:**
```go
type pushNotificationEvent struct {
    ID       string               `json:"id"`
    Username string               `json:"username"`  // Lookup key for device tokens
    Title    string               `json:"title"`
    Body     string               `json:"body"`
    Data     pushNotificationData `json:"data"`
    RoomID   string               `json:"roomId"`
}

type pushNotificationData struct {
    RoomID            string `json:"rid"`
    MessageID         string `json:"messageId"`
    Type              string `json:"type"`
    ChineseName       string `json:"chineseName"`
    EngName           string `json:"engName"`
    ThreadMessageID   string `json:"tmid,omitempty"`
    FileName          string `json:"fileName,omitempty"`
    FileType          string `json:"fileType,omitempty"`
    ParentRoomID      string `json:"prid,omitempty"`
    PushTime          string `json:"pushTime"`
    AlsoSendToChannel bool   `json:"alsoSendToChannel,omitempty"`
}
```

### 2.5 Push Notification Consumer Service

**Architecture:**

```text
┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│   JetStream     │────▶│   JetStreamHandler   │────▶│   WorkerPool    │
│   (subjects)    │     │   (queue/jetstream)  │     │   (concurrency) │
└─────────────────┘     └──────────────────────┘     └─────────────────┘
                                                            │
                                                            ▼
┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│  Push Provider  │◀────│   notify.PushService │◀────│      task       │
│   (via HTTP)    │     │   (notify/)          │     │   (queue/task)  │
└─────────────────┘     └──────────────────────┘     └─────────────────┘
```

**Key Components:**

1. **Entry Point** - Creates PushService, initializes JetStreamHandler
   - Supports multiple stream/subject pairs
   - Configurable worker pool size

2. **Queue Handler**
   - Subscribes to multiple JetStream subjects
   - Uses concurrent worker pool
   - Supports consumer retry with backoff
   - Ack policy: `AckExplicitPolicy`
   - Deliver policy: `DeliverNewPolicy`
   
   **Consumer Config:**
   ```go
   durable := fmt.Sprintf("%s-%s", division, stream)
   nats.WithFilterSubject(subject),
   nats.WithAckPolicy(nats.AckExplicitPolicy),
   nats.WithDeliverPolicy(nats.DeliverNewPolicy),
   nats.WithAckWait(cfg.JetStream.Consumer.AckWait),
   nats.WithBackOff(cfg.JetStream.Consumer.BackOff),
   nats.WithMaxDeliver(cfg.JetStream.Consumer.MaxDeliver),
   ```

3. **PushService Interface**
   ```go
   type PushService interface {
       Push(*Event) (retry bool, err error)
       Name() string
   }
   
   type Event struct {
       ID       string      `json:"id"`
       Username string      `json:"username"`  // Lookup key for device tokens
       Title    string      `json:"title"`
       Body     string      `json:"body"`
       Data     interface{} `json:"data"`
       RoomID   string      `json:"roomId"`
   }
   ```

**Token Resolution:**
The push service receives `username` in the event and queries an internal service to get device tokens.

---

## 3. User Presence Service

### 3.1 Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────┐
│                      User Presence Service                      │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐   │
│  │  HTTP API    │  │   NATS Pub   │  │   NATS Sub           │   │
│  └──────────────┘  └──────────────┘  └──────────────────────┘   │
│         │                 │                    │                │
│         ▼                 ▼                    ▼                │
│  ┌────────────────────────────────────────────────────────┐    │
│  │             Presence Service                            │    │
│  │  - Tracks user connections                             │    │
│  │  - Publishes presence updates                          │    │
│  │  - Uses state machine for status aggregation           │    │
│  └────────────────────────────────────────────────────────┘    │
│         │                                                       │
│         ▼                                                       │
│  ┌────────────────────────────────────────────────────────┐    │
│  │      Valkey Cache + Domain Models                      │    │
│  │  - Status: "online", "offline", "away", "busy", "in-call"│   │
│  │  - ManualStatus: manual overrides                       │    │
│  │  - ConnectionDetail: per-connection tracking           │    │
│  │  - State machine aggregates multiple connections       │    │
│  └────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Data Model

```go
// Aggregated status (computed from connections + manual status)
const (
    UserStatusOnline  = "online"
    UserStatusAway    = "away"
    UserStatusBusy    = "busy"
    UserStatusOffline = "offline"
    UserStatusInACall = "in-call"
)

// User manual status overrides
const (
    ManualStatusOnline  = "MANUAL_ONLINE"
    ManualStatusAway    = "MANUAL_AWAY"
    ManualStatusBusy    = "MANUAL_BUSY"
    ManualStatusOffline = "MANUAL_OFFLINE"
)

type UserPresence struct {
    AccountName      string    `json:"account_name"`
    AggregatedStatus string    `json:"aggregated_status"`  // One of UserStatus*
    LastUpdated      time.Time `json:"last_updated"`
}

type ManualStatus struct {
    AccountName string    `json:"account_name"`
    Status      string    `json:"status"`  // One of ManualStatus*
    SetAt       time.Time `json:"set_at"`
}

type ConnectionDetail struct {
    AccountName string    `json:"account_name"`
    ConnID      string    `json:"conn_id"`
    LastUpdated time.Time `json:"last_updated"`
}

type CallStatus struct {
    AccountName string
    InACall     bool  // From external integration
    Overwritten bool
}
```

### 3.3 How Presence is Used for Notifications

The notification validator doesn't query presence service directly. Instead, **presence data is embedded in the User struct** passed to notifications:

**From the subscription query, the receiver includes:**
```go
type User struct {
    // ... other fields
    Status           string  // "online", "offline", "away", "busy"
    StatusConnection string  // Connection state
}
```

**In validator:**
```go
if params.Receiver.StatusConnection == "offline" ||
   params.Receiver.Status == "busy" {
   return false
}
```

**Key Pattern:** Presence is checked **against cached user data pulled during the subscription query**, not via real-time presence service calls.

---

## 4. Gaps Between Legacy and New Backend

### 4.1 Architecture Differences

| Aspect | Legacy System | New Backend |
|--------|---------------|-------------|
| **Message Fan-out** | Hook handler queries Mongo inline | Notification-worker uses `roomsubcache` |
| **Notification Logic** | Inline in hook-handler | Separate notification-worker service |
| **Presence Checking** | Embedded in User struct from Mongo query | Pluggable `StatusChecker` interface |
| **Mobile Push** | Via JetStream to push service | Needs hand-off interface |
| **Desktop Notification** | Via horizontal broadcast | Via NATS `chat.user.{account}.notification` |
| **Hook Handler** | Real hooks with multiple handlers | Pluggable `Hook` interface (no-op default) |

### 4.2 Open Questions Answered by Legacy System

#### **Question A: Push Notification Service Interface**

**Based on legacy system:**
```go
// Transport: JetStream
// Worker publishes to subject consumed by push-notification service

type pushNotificationEvent struct {
    ID       string               `json:"id"`
    Username string               `json:"username"`
    Title    string               `json:"title"`
    Body     string               `json:"body"`
    Data     pushNotificationData `json:"data"`
    RoomID   string               `json:"roomId"`
}

type pushNotificationData struct {
    RoomID            string `json:"rid"`
    MessageID         string `json:"messageId"`
    Type              string `json:"type"`
    ChineseName       string `json:"chineseName"`
    EngName           string `json:"engName"`
    ThreadMessageID   string `json:"tmid,omitempty"`
    FileName          string `json:"fileName,omitempty"`
    FileType          string `json:"fileType,omitempty"`
    ParentRoomID      string `json:"prid,omitempty"`
    PushTime          string `json:"pushTime"`
    AlsoSendToChannel bool   `json:"alsoSendToChannel,omitempty"`
}

// Flow:
// notification-worker ─JetStream▶ push-notification service ─HTTP▶ Push Provider
```

**Recommendations:**
- **A1:** JetStream stream (not NATS subject directly)
- **A2:** Stream = `PUSH_NOTIFICATIONS_{siteID}`, owned by push service
- **A3:** Schema as above
- **A4:** Worker sends username (push service resolves device tokens)
- **A5:** Fire-and-forget (consumer acks, worker doesn't wait)
- **A6:** Per-user (trivial batching not worth complexity)

#### **Question B: User Presence / Status Service**

**Based on legacy system:**
```go
// Presence service stores in Valkey:
type UserPresence struct {
    AccountName      string    `json:"account_name"`
    AggregatedStatus string    `json:"aggregated_status"`  // "online", "offline", "away", "busy", "in-call"
    LastUpdated      time.Time `json:"last_updated"`
}

type ManualStatus struct {
    AccountName string    `json:"account_name"`
    Status      string    `json:"status"`  // "MANUAL_ONLINE", etc
    SetAt       time.Time `json:"set_at"`
}
```

**Query Method Options:**
1. HTTP API (presence service provides REST endpoint)
2. NATS Request/Reply (presence service subscribes to subject)
3. Direct Valkey read (notification-worker reads from Valkey directly)

**Legacy approach:** Pre-embedded in subscription query

**Recommendations:**
- **B1:** Either HTTP API or NATS request/reply
- **B2:** States: `online`, `offline`, `away`, `busy`, `in-call`
- **B3:** Must be inline (on hot path). Target <5ms. Presence service caches in Valkey.
- **B4:** **Fail-open** (notify) - never drop notification due to presence check failure

**Interface Design:**
```go
type StatusChecker interface {
    // ShouldNotify returns the preferred delivery channels based on presence
    // Returns (desktop bool, mobile bool, err error)
    ShouldNotify(ctx context.Context, userID string) (desktop, mobile bool, err error)
}
```

#### **Question C: Desktop vs Mobile Routing**

**Based on legacy system:**

**Logic for desktop:**
```go
// Skip if:
// - User offline OR busy
// - User disabled desktop notifications
// - Large room AND not mention/thread/DM

// Send if:
// - DM room
// - Mentions (@all, @here, direct)
// - Highlighted message
// - User preference is "all"
```

**Logic for mobile:**
```go
// Skip if:
// - Push disabled globally
// - Large room AND not mention/DM/mention-all
// - User is bot

// Note: Unlike desktop, mobile doesn't check presence or user preferences
// This might be a design limitation
```

**Recommendations:**
- **C1:** Online on desktop → desktop only; offline → mobile push; busy → skip both
  - Both legs should not fire simultaneously
- **C2:** Mute (`DisableNotification`) suppresses **both** legs
- **C3:** Restricted room check applies to **both** legs

#### **Question D: Hook Handler**

**Based on legacy system:**

The legacy hook handler has **multiple responsibilities** spread across services in the new architecture:

**Legacy Hook-Handler Handlers:**
1. `updateSubscriptions` - Updates subscription document
2. `handleThreadMessages` - Manages thread reply tracking
3. `sendNotifications` - Desktop + mobile notifications
4. `updateRoomToNotifyUsers` - Updates room metadata
5. `publishBotPlatformEvent` - Bot platform integration
6. `publishFederationEvent` - Cross-domain forwarding

**In new architecture, these are split:**
- `broadcast-worker` → Rooms cache update
- `room-service` → Subscription management
- `notification-worker` → Notifications
- `bot-service` → Bot platform integration
- `federation-worker` → Cross-domain forwarding

**Hook Handler Purpose for Notification-Worker:**
The hook handler should be a **notification-specific predicate** that can:
- Decide if notification should be sent (suppress-only)
- NOT modify content (broadcast-worker owns encryption)
- Run BEFORE mobile push (can't undo push)

**Interface Design:**
```go
type Hook interface {
    // Allow returns true if notification should proceed
    // Called once per recipient, can inspect message + subscription
    // Errors are logged but don't block notification (fail-open)
    Allow(ctx context.Context, evt model.NotificationEvent, member roomsubcache.Member) (bool, error)
}
```

**D1:** Decides whether to send notification + which channels (suppress-only)  
**D2:** NATS request/reply (external call - hook may be complex)  
**D3:** Suppress-only (content modification owned by broadcast-worker)  
**D4:** Inputs: full notification event + member info; Failure: log + allow (fail-open); Run: BEFORE status check

#### **Question E: Cache Freshness**

**Based on legacy system:**

The legacy system queries MongoDB inline (no cache). However, cache invalidation events exist in the new system:

**Inbox events for member changes:**
```go
type InboxMemberEvent struct {
    RoomID             string   `json:"roomId"`
    RoomName           string   `json:"roomName"`
    RoomType           RoomType `json:"roomType"`
    SiteID             string   `json:"siteId"`
    Accounts           []string `json:"accounts"`
    HistorySharedSince *int64   `json:"historySharedSince,omitempty"`  // KEY FOR RESTRICTED ROOM
    JoinedAt           int64    `json:"joinedAt,omitempty"`
    Timestamp          int64    `json:"timestamp" bson:"timestamp"`
}
```

The `HistorySharedSince` field in `InboxMemberEvent` is the source for restricted room membership.

**Recommendations:**
- **E1:** Default TTL = 5 minutes (300s). Configurable via `ROOMSUBCACHE_TTL_SECONDS`
- **E2:** Events that invalidate cache:
  - `InboxMemberEvent` (member add/remove) - `HistorySharedSince` changes
  - Mute toggle event
  - Disable notification event

---

## 5. Required Implementation for Notification Worker

### 5.1 Files to Add/Modify

#### **1. Mobile Push Emitter** (`emit.go`)
```go
package main

import (
     "context"
    "encoding/json"
    "fmt"
    
    "github.com/nats-io/nats.go/jetstream"
    "github.com/hmchangw/chat/pkg/model"
)

type mobileEmitter struct {
    js     jetstream.JetStream
    stream string
}

func NewMobileEmitter(js jetstream.JetStream, streamName string) Emitter {
    return &mobileEmitter{js: js, stream: streamName}
}

func (e *mobileEmitter) Emit(ctx context.Context, evt model.NotificationEvent, account string) error {
    pushEvt := pushNotificationEvent{
        ID:       fmt.Sprintf("%s-%s", evt.Message.ID, account),
        Username: account,
        Title:    evt.Message.RoomName,
        Body:     truncate(evt.Message.Content, 1000),
        Data: pushNotificationData{
            RoomID:    evt.RoomID,
            MessageID: evt.Message.ID,
            Type:      evt.Message.RoomType,
            // ... other fields
        },
    }
    
    data, _ := json.Marshal(pushEvt)
    _, err := e.js.Publish(ctx, fmt.Sprintf("push.%s", account), data)
    return err
}
```

#### **2. Presence Checker Implementation** (`presence.go`)

**Option A: HTTP Client**
```go
package main

type httpStatusChecker struct {
    baseURL string
    client  *http.Client
}

func (c *httpStatusChecker) ShouldNotify(ctx context.Context, userID string) (desktop, mobile bool, err error) {
    req, _ := http.NewRequestWithContext(ctx, "GET", 
        c.baseURL+"/presence/"+userID, nil)
    resp, err := c.client.Do(req)
    if err != nil {
        return true, true, nil  // Fail-open
    }
    defer resp.Body.Close()
    
    var presence struct {
        Status           string `json:"status"`
        StatusConnection string `json:"statusConnection"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&presence); err != nil {
        return true, true, nil  // Fail-open
    }
    
    // Logic: online → desktop; offline → mobile; busy → neither
    isOnline := presence.StatusConnection == "online" && presence.Status != "busy"
    isOffline := presence.StatusConnection == "offline"
    
    return isOnline, isOffline, nil
}
```

**Option B: Valkey Reader** (faster, no HTTP overhead)
```go
package main

type valkeyStatusChecker struct {
    client valkeyutil.Client
}

func (c *valkeyStatusChecker) ShouldNotify(ctx context.Context, userID string) (desktop, mobile bool, err error) {
    data, err := c.client.Get(ctx, "presence:"+userID)
    if err != nil {
        return true, true, nil  // Fail-open
    }
    
    var presence UserPresence
    if err := json.Unmarshal([]byte(data), &presence); err != nil {
        return true, true, nil
    }
    
    isBusy := presence.AggregatedStatus == "busy"
    isOffline := presence.AggregatedStatus == "offline"
    isOnline := presence.AggregatedStatus == "online" && !isBusy
    
    return isOnline, isOffline, nil
}
```

#### **3. RoomSubcache Member Extension**

```go
// In pkg/roomsubcache/roomsubcache.go

type Member struct {
    ID                  string `json:"id"`
    Account             string `json:"account"`
    DisableNotification bool   `json:"disableNotification,omitempty"`
    HistorySharedSince  *int64 `json:"historySharedSince,omitempty"`
}
```

**The cache loader needs to populate these fields** (in `members.go`):
```go
func (m *cachedMemberLookup) loadFromMongo(ctx context.Context, roomID string) ([]roomsubcache.Member, error) {
    subs, err := m.col.Find(ctx, bson.M{"roomId": roomID}, options.Find().SetProjection(bson.M{
        "u._id": 1,
        "u.account": 1,
        "disableNotification": 1,
        "historySharedSince": 1,
    }))
    // ... convert to []roomsubcache.Member
}
```

#### **4. Filter Logic** (in `handler.go`)

```go
func (h *Handler) HandleMessage(ctx context.Context, data []byte) error {
    var evt model.MessageEvent
    if err := json.Unmarshal(data, &evt); err != nil {
        return fmt.Errorf("unmarshal message event: %w", err)
    }
    
    members, err := h.members.GetMembers(ctx, evt.Message.RoomID)
    if err != nil {
        return fmt.Errorf("get members for room %s: %w", evt.Message.RoomID, err)
    }
    
    for _, member := range members {
        // Skip sender
        if member.ID == evt.Message.UserID {
            continue
        }
        
        // Skip muted users
        if member.DisableNotification {
            continue
        }
        
        // Skip restricted room access
        if member.HistorySharedSince != nil {
            sharedSince := time.UnixMilli(*member.HistorySharedSince).UTC()
            if evt.Message.CreatedAt.Before(sharedSince) {
                continue
            }
        }
        
        // Check hook handler (suppress-only)
        if h.hook != nil {
            allowed, err := h.hook.Allow(ctx, evt, member)
            if err != nil {
                slog.Error("hook check failed", "error", err, "user", member.ID)
            } else if !allowed {
                continue
            }
        }
        
        // Check presence - which channels to notify
        desktop, mobile := true, true
        if h.statusChecker != nil {
            var err error
            desktop, mobile, err = h.statusChecker.ShouldNotify(ctx, member.ID)
            if err != nil {
                slog.Error("presence check failed", "error", err, "user", member.ID)
            }
        }
        
        // Emit notifications
        notif := model.NotificationEvent{...}
        
        if desktop {
            if err := h.desktopEmitter.Emit(ctx, notif, member.Account); err != nil {
                slog.Error("desktop emit failed", ...)
            }
        }
        
        if mobile {
            if err := h.mobileEmitter.Emit(ctx, notif, member.Account); err != nil {
                slog.Error("mobile emit failed", ...)
            }
        }
    }
    
    return nil
}
```

### 5.2 Dependencies on Other Services

| Service | Purpose | Integration Point |
|---------|---------|------------------|
| **user-presence-service** | Presence/status checking | HTTP API or Valkey |
| **push-notification** | Final mobile push delivery | JetStream producer |
| **hook-handler** (optional) | Custom notification predicates | NATS or HTTP |
| **roomsubcache** | Room membership cache | Direct package import |
| **Valkey** | Cache storage | Valkey client |
| **MongoDB** | Cache fallback (subscriptions) | Mongo driver |
| **NATS/JetStream** | Message consumption + publish | NATS client |

---

## 6. Recommended Spec Updates

### 6.1 Answer All Open Questions

**A. Push Notification Service**
- **A1:** JetStream stream
- **A2:** Stream = `PUSH_NOTIFICATIONS_{siteID}`, owned by push service
- **A3:** Schema = `pushNotificationEvent`
- **A4:** Worker sends username
- **A5:** Fire-and-forget
- **A6:** Per-user

> **Note (historical / non-normative).** Sections **B**, **C**, and **D**
> below record the initial answers explored during spec review. They have
> been **superseded** by the canonical design in
> `2026-05-22-notification-worker-cache-and-mobile-design.md` — defer to
> that document for the authoritative contracts (bulk presence RPC, the
> in-process routing predicate, and the suppress-only hook). The bullets
> are kept here only as a record of the analysis that fed the final
> design.

**B. User Presence Service**
- **B1:** HTTP API or read Valkey directly
- **B2:** States: `online`, `offline`, `away`, `busy`, `in-call`
- **B3:** Inline, target <5ms
- **B4:** Fail-open

**C. Desktop vs Mobile Routing**
- **C1:** Online → desktop; offline → mobile; busy → neither
- **C2:** DisableNotification suppresses both legs
- **C3:** Restricted room applies to both legs

**D. Hook Handler**
- **D1:** Suppress-only predicate
- **D2:** NATS request/reply
- **D3:** Suppress-only (content modification separate)
- **D4:** Run before presence check

**E. Cache Freshness**
- **E1:** TTL = 300s (configurable)
- **E2:** Invalidate on member events, mute toggles

### 6.2 Add to Design Document

1. Interface definitions for StatusChecker and Hook
2. Concrete implementation sketches
3. Error handling strategy (fail-open)
4. Sequence diagram showing full flow
5. Configuration spec
6. Testing strategy

### 6.3 Create Implementation Plan

After spec approval:
1. Phase 1: Cache integration + bug fixes
2. Phase 2: Mobile push emitter
3. Phase 3: Presence checking
4. Phase 4: Hook handler integration

---

## 7. Legacy Notification System Summary

**Service:** Hook-handler with after-save handlers  
**Entry Point:** sendNotifications handler  
**Trigger:** Persisted messages via JetStream  

### Key Behaviors:
1. **Fan-out logic:**
   - Skips sender
   - Skips bots (for mobile push)
   - Skips muted users
   - Skips large room non-mentions (configurable threshold)
   - Skips restricted room threads

2. **Notification channels:**
   - **Desktop:** Via horizontal broadcast
   - **Mobile:** Via push-notification service

3. **Presence checking:**
   - Uses cached user status from subscription query
   - Filters: `StatusConnection=offline` OR `Status=busy` → skip desktop
   - Mobile push ignores presence status

4. **Preferences:**
   - Desktop: Uses subscription preference or defaults
   - Audio: Uses subscription preference or defaults
   - Mobile: No preference filtering

5. **Complex logic:**
   - Highlights (custom keywords)
   - Thread following
   - @all/@here vs direct mentions
   - DM rooms always notify (except muted)

### Migration Notes:
- The subscription query does heavy lifting
- Thread handling requires parent message lookup
- Federation and bot platform are separate concerns

---

## 8. Implementation Roadmap: What to Copy vs Adapt

> **Note (historical / non-normative).** The COPY / ADAPT / SKIP guidance
> below reflects the early analysis phase. The canonical design spec
> (`2026-05-22-notification-worker-cache-and-mobile-design.md`) is the
> authoritative source for what to build — in particular for presence
> (bulk RPC, not per-recipient HTTP or direct Valkey), routing (in-process
> predicate, not per-recipient RPC), and the hook contract. Treat this
> section as background reading, not an implementation checklist.

**Critical guidance for future development:** Based on this analysis, here's exactly what needs to be built and what can be reused.

### 8.1 Components to COPY (Build New, Inspired by Legacy)

These implementations should be created fresh in your new backend, following legacy patterns but using your architecture:

| Component | From Legacy | To Your Backend | Notes |
|-----------|-------------|-----------------|-------|
| **pushNotificationEvent Schema** | `hooks/models.go` | `pkg/model/push.go` | Copy structure, remove `ID` field (use message ID), adapt field names to your patterns |
| **Hook Notification Validator Logic** | `pkg/notification/validate.go` | `notification-worker/validator.go` | Copy `shouldNotify`, `shouldNotifyDesktop`, `shouldNotifyAudio` logic. Adapt to use `StatusChecker` interface |
| **Mobile Push Filtering** | `after_save_message.go:293-310` | `notification-worker/handler.go` | Copy the filtering logic for push notifications (bot check, DM vs channel logic) |
| **Push Event Builder** | `after_save_message.go:357-389` | `notification-worker/emit.go` | Copy the event construction logic. Adapt `Participant` struct to your `pkg/model` version |
| **Worker Pool Pattern** | `queue/jetstream.go` | Your worker already has this | Use same pattern your services already have (pull iterator + semaphore) |
| **Push Notification Config** | `app/push-notification/internal/config/` | `push-notification-service/` | New service needed. Copy consumer config patterns |

### 8.2 Components to ADAPT (Modify Legacy Code)

These need significant changes to work in your architecture:

| Component | Legacy Approach | Your Approach | Required Changes |
|-----------|----------------|---------------|------------------|
| **sendNotifications Handler** | Inline in hook-handler | Standalone JetStream consumer | Extract fan-out loop from hook-handler. Add `roomsubcache` integration. Remove subscription query Mongo logic |
| **Member Lookup** | Direct Mongo query | `roomsubcache` + Mongo fallback | Rewrite to use `pkg/roomsubcache`. Load only: `ID`, `Account`, `DisableNotification`, `HistorySharedSince` |
| **Presence Checking** | Pre-embedded in User struct | Live `StatusChecker` call | Create interface. Either HTTP to presence-service OR Valkey read. Must be async on hot path |
| **Subscription Query Filter** | Complex MongoDB aggregation | Pre-computed in cache | **Don't port this!** Cache projections with only notification fields |
| **Hook Handler** | Callback list in hook-handler | Pluggable `Hook` interface | Extract suppression logic. Define interface. Keep no-op default |
| **Thread Handling** | Parent message lookup | Stream message metadata | In new architecture, check if thread metadata is in `MessageEvent` before diving deeper |

### 8.3 Components to SKIP (Not Needed in New Architecture)

| Legacy Component | Why Not Needed | Alternative in New Backend |
|-----------------|----------------|---------------------------|
| `updateSubscriptions` handler | `room-service` handles subscriptions | N/A (separate service) |
| `publishBotPlatformEvent` | `bot-service` handles bots | Handled in bot service |
| `publishFederationEvent` | `federation-worker` handles cross-site | Use `inbox-worker` pattern |
| Direct Mongo queries for subscriptions | `roomsubcache` is the source of truth | Use cache exclusively |
| `MessageOutboxRepo` for request timing | Use JetStream metadata or remove | Optional metric |
| Bot service integration | Separate service concern | Don't mix into notification-worker |
| HR/Employee lookups | Authorization service concern | Notifications don't need this |

### 8.4 Specific Code Patterns to Copy

**From `validate.go` - Presence Logic:**
```go
// Copy this logic exactly, adapt struct names:
if receiver.StatusConnection == "offline" || receiver.Status == "busy" {
    return false  // Skip notification
}

// DM rooms always notify
if room.Type == "d" {
    return true
}

// Mention logic
if hasMentionToUser || hasMentionToAll || hasMentionToHere || isHighlighted {
    return true
}

// Large room non-mention filtering
if disableAllMessageNotifications {
    return false
}
```

**From `after_save_message.go` - Push Filtering:**
```go
// Copy this logic into mobile emitter
if !room.IsDirectMessage() && 
   !hasMentionToUser && 
   !hasMentionToAll && 
   disableAllMessageNotifications {
    return nil  // Skip push for normal messages in large rooms
}

if IsBotUser(member.Account) {
    return nil  // Don't push to bots
}
```

**From `after_save_message.go` - Event Construction:**
```go
// Adapt this pattern in emit.go
pushEvent := model.PushNotificationEvent{
    ID:       fmt.Sprintf("%s-%s", evt.Message.ID, member.Account),
    Username: member.Account,
    Title:    getRoomName(evt), // Helper needed
    Body:     truncate(evt.Message.Content, maxLen),
    Data: model.PushNotificationData{
        RoomID:          evt.RoomID,
        MessageID:       evt.Message.ID,
        ChineseName:     evt.Sender.ChineseName,
        EngName:         evt.Sender.EngName,
        // ... etc
    },
}
```

### 8.5 Architecture Changes Summary

**Key Differences During Implementation:**

1. **Fan-out source:**
   - Legacy: Query Mongo subscriptions
   - New: Read from `roomsubcache` (Valkey)
   - Impact: Much faster, but needs TTL handling

2. **Presence checking:**
   - Legacy: Embedded in User struct at query time
   - New: Live check via `StatusChecker` interface
   - Impact: More accurate but adds latency

3. **Desktop delivery:**
   - Legacy: Horizontal broadcast to separate service
   - New: Direct NATS publish
   - Impact: Simpler, less coupling

4. **Mobile delivery:**
   - Legacy: Via hook-handler then push service
   - New: Direct to push service via JetStream
   - Impact: Simpler path, fewer hops

5. **Hook handling:**
   - Legacy: In-process callbacks
   - New: Interface (default no-op)
   - Impact: More flexible, but code to maintain

### 8.6 Integration with Other Services

Your notification-worker will integrate with:

**Required:**
- `roomsubcache` (via `pkg/roomsubcache`) - Member list source
- `Valkey` - Cache storage
- `MongoDB` - Cache fallback
- `NATS/JetStream` - Message consumption + desktop/mobile publish
- `push-notification service` - Mobile push delivery

**Optional:**
- `user-presence-service` - For presence checking (can be no-op initially)
- `hook-handler` - For custom notification predicates (no-op default)

**Not needed:**
- Direct subscription modifications (handled by `room-service`)
- Bot platform integration (handled by `bot-service`)
- Federation forwarding (handled by `federation-worker`)

### 8.7 Development Priority

**Phase 1: Core Functionality (cache + basic filtering)**
1. Extend `roomsubcache.Member` with `DisableNotification` and `HistorySharedSince`
2. Implement `cachedMemberLookup` with cache miss handling
3. Update `handler.go` with mute/restricted filtering
4. Test cache integration

**Phase 2: Desktop Notifications (existing path)**
1. Desktop emitter implementation
2. Hook handler interface (no-op default)
3. Status checker interface (no-op default)
4. Full handler orchestration
5. Integration tests

**Phase 3: Mobile Push (new path)**
1. Push notification event models
2. Mobile emitter implementation
3. Push service consumer setup
4. End-to-end mobile push tests

**Phase 4: Advanced Features (presence + hooks)**
1. Presence service integration
2. Real hook handler implementation (optional)
3. Performance optimization
4. Cache invalidation listener

---

**End of Analysis Document**

This document provides the missing pieces needed to complete the design spec, anonymized from a legacy enterprise chat system implementation. Use Section 8 as a checklist during implementation to ensure nothing critical is missed from the legacy system while staying true to the new microservice architecture.
