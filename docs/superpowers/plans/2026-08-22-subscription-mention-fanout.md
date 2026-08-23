# Cross-Site Mention Badge Fan-Out Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fan a room-level `@`-mention out to every mentioned user's home site so a federated user's `hasMention` badge is set on the site they actually read from.

**Architecture:** `broadcast-worker` already resolves each mentionee to a `model.Participant` carrying `SiteID`. After the client fan-out it groups those participants by site, skips local/blank, and publishes one `subscription_mention` OUTBOX event per remote site. `outbox-worker` forwards it (unchanged — it is generic over event type) into the destination's INBOX, where `inbox-worker` applies the same guarded `hasMention` write the origin performs.

**Tech Stack:** Go 1.25, NATS JetStream, MongoDB (`go.mongodb.org/mongo-driver/v2`), `go.uber.org/mock`, `testify`, `testcontainers-go` via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-08-22-subscription-mention-fanout-design.md`

> **Note.** This plan was written before implementation. The review passes that
> followed renamed the handler's publisher field, replaced the two hand-rolled
> dedup-ID formats with `natsutil.InboxDedupID` (`{requestID}:{destSiteID}`),
> made the per-site map lazily allocated, and moved the edit-path user lookup off
> the user-visible latency path. The snippets below show the planned shape, not
> the merged one — the spec and the code are authoritative.

## Global Constraints

- Run `make` targets only — never raw `go` commands. `make lint`, `make test`, `make test SERVICE=<name>`, `make generate SERVICE=<name>`, `make test-integration SERVICE=<name>`, `make sast`.
- TDD is mandatory: write the failing test, run it, confirm it fails, then implement.
- All NATS payload structs live in `pkg/model` with both `json` and `bson` camelCase tags, and every event struct carries `Timestamp int64`.
- Event timestamps are set at the publish site with `time.Now().UTC().UnixMilli()`.
- Errors wrap with context: `fmt.Errorf("short description: %w", err)`. Never bare `err`.
- Logging is `log/slog` with structured key-value fields. Never log message bodies or tokens — accounts and IDs only.
- Subjects come from `pkg/subject` builders, never `fmt.Sprintf`.
- `broadcast-worker` marshals with `github.com/bytedance/sonic`; `inbox-worker` and `pkg/outbox` use `encoding/json`.
- Comments are short and neat: max two lines, explaining WHY not WHAT.
- Minimum 80% coverage; target 90%+ for handlers.

---

### Task 1: Contract — event type, payload struct, OUTBOX lane

**Files:**
- Modify: `pkg/model/event.go` (const block at `:156-174`, and the event structs below it)
- Modify: `pkg/outbox/outbox.go` (`ConcurrentEventTypes` at `:20-43`)
- Test: `pkg/model/model_test.go`, `pkg/outbox/outbox_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `model.InboxSubscriptionMention InboxEventType = "subscription_mention"`
  - `model.SubscriptionMentionEvent{RoomID string; Accounts []string; MentionedAt int64; Timestamp int64}`

- [ ] **Step 1: Write the failing tests**

Append to `pkg/outbox/outbox_test.go`:

```go
func TestSubscriptionMentionIsConcurrent(t *testing.T) {
	assert.Contains(t, ConcurrentEventTypes, model.InboxSubscriptionMention,
		"subscription_mention is order-insensitive (the destination write is a guarded idempotent $set) so it rides the concurrent lane")
	assert.NotContains(t, OrderedEventTypes, model.InboxSubscriptionMention)
}
```

Append to `pkg/model/model_test.go`:

```go
func TestSubscriptionMentionEvent_RoundTrip(t *testing.T) {
	src := &SubscriptionMentionEvent{
		RoomID:           "room-1",
		Accounts:         []string{"alice", "bob"},
		MentionedAt: 1755820800000,
		Timestamp:        1755820800123,
	}
	roundTrip(t, src, &SubscriptionMentionEvent{})
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=pkg/outbox` then `make test SERVICE=pkg/model`
Expected: compile failure — `undefined: model.InboxSubscriptionMention`, `undefined: SubscriptionMentionEvent`.

- [ ] **Step 3: Add the event type constant**

In `pkg/model/event.go`, inside the `InboxEventType` const block, after `InboxSubscriptionSectionMoved`:

```go
	InboxSubscriptionMention         InboxEventType = "subscription_mention"
```

- [ ] **Step 4: Add the payload struct**

In `pkg/model/event.go`, next to the other inbox payload structs:

```go
// SubscriptionMentionEvent federates a room-level @-mention badge to the
// mentionees' home site. Accounts holds only the accounts homed at the
// destination, so no site learns mention identities it has no business seeing.
type SubscriptionMentionEvent struct {
	RoomID   string   `json:"roomId"   bson:"roomId"`
	Accounts []string `json:"accounts" bson:"accounts"`
	// MentionedAt is when the mention appeared — createdAt, or editedAt when an
	// edit added it — in unix-millis; it feeds the already-read guard.
	MentionedAt int64 `json:"mentionedAt" bson:"mentionedAt"`
	Timestamp        int64 `json:"timestamp"        bson:"timestamp"`
}
```

- [ ] **Step 5: Add the type to the concurrent lane**

In `pkg/outbox/outbox.go`, at the end of the `ConcurrentEventTypes` slice:

```go
	// broadcast-worker: room-level mention badge. The destination write is a
	// guarded idempotent $set, so duplicates and out-of-order applies converge.
	model.InboxSubscriptionMention,
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `make test SERVICE=pkg/outbox` then `make test SERVICE=pkg/model`
Expected: PASS, including the pre-existing `TestEventTypeSetsAreDisjoint`.

- [ ] **Step 7: Commit**

```bash
git add pkg/model/event.go pkg/model/model_test.go pkg/outbox/outbox.go pkg/outbox/outbox_test.go
git commit -m "feat(model,outbox): add subscription_mention federation event"
```

---

### Task 2: Consumer — `inbox-worker` applies the federated mention

**Files:**
- Modify: `inbox-worker/handler.go` (`InboxStore` interface, `HandleEvent` switch at `:144-183`)
- Modify: `inbox-worker/main.go` (`mongoInboxStore` methods, struct at `:57-63`)
- Test: `inbox-worker/handler_test.go`, `inbox-worker/integration_test.go`
- Regenerate: `inbox-worker/mock_store_test.go`

**Interfaces:**
- Consumes: `model.InboxSubscriptionMention`, `model.SubscriptionMentionEvent` (Task 1).
- Produces: `InboxStore.SetSubscriptionMentions(ctx context.Context, roomID string, accounts []string, msgCreatedAt time.Time) error`.

- [ ] **Step 1: Write the failing handler tests**

Append to `inbox-worker/handler_test.go`:

```go
func TestHandler_HandleSubscriptionMention(t *testing.T) {
	msgAt := time.UnixMilli(1755820800000).UTC()

	tests := []struct {
		name      string
		payload   any
		storeErr  error
		wantErr   string
		wantStore bool
	}{
		{
			name: "applies the badge to the destination accounts",
			payload: model.SubscriptionMentionEvent{
				RoomID: "room-1", Accounts: []string{"alice", "bob"},
				MentionedAt: msgAt.UnixMilli(), Timestamp: msgAt.UnixMilli(),
			},
			wantStore: true,
		},
		{
			name:    "malformed payload",
			payload: "not-an-object",
			wantErr: "unmarshal subscription_mention payload",
		},
		{
			name: "store error propagates for redelivery",
			payload: model.SubscriptionMentionEvent{
				RoomID: "room-1", Accounts: []string{"alice"},
				MentionedAt: msgAt.UnixMilli(), Timestamp: msgAt.UnixMilli(),
			},
			storeErr:  errors.New("mongo down"),
			wantStore: true,
			wantErr:   "set subscription mentions",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockInboxStore(ctrl)
			if tc.wantStore {
				e := tc.payload.(model.SubscriptionMentionEvent)
				store.EXPECT().
					SetSubscriptionMentions(gomock.Any(), e.RoomID, e.Accounts, msgAt).
					Return(tc.storeErr)
			}

			payload, err := json.Marshal(tc.payload)
			require.NoError(t, err)
			data, err := json.Marshal(model.InboxEvent{
				Type:       model.InboxSubscriptionMention,
				SiteID:     "site-a",
				DestSiteID: "site-b",
				Payload:    payload,
				Timestamp:  msgAt.UnixMilli(),
			})
			require.NoError(t, err)

			err = NewHandler(store).HandleEvent(context.Background(), data)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=inbox-worker`
Expected: compile failure — `NewMockInboxStore` has no `SetSubscriptionMentions`.

- [ ] **Step 3: Add the store method to the interface**

In `inbox-worker/handler.go`, at the end of the `InboxStore` interface:

```go
	// SetSubscriptionMentions flags accounts as mentioned in roomID, skipping any
	// that already read past msgCreatedAt so a federated badge can't clobber a
	// read-clear that landed first (#467). A non-subscriber simply matches nothing.
	SetSubscriptionMentions(ctx context.Context, roomID string, accounts []string, msgCreatedAt time.Time) error
```

- [ ] **Step 4: Add the dispatch case and handler**

In `inbox-worker/handler.go`, in the `HandleEvent` switch after the `model.InboxSubscriptionSectionMoved` case:

```go
	case model.InboxSubscriptionMention:
		return h.handleSubscriptionMention(ctx, &evt)
```

And the handler itself, next to `handleSubscriptionRead`:

```go
// handleSubscriptionMention replicates a room-level @-mention badge onto the
// mentionees' home replicas. The store's already-read guard makes it idempotent
// and order-safe against a concurrent subscription_read.
func (h *Handler) handleSubscriptionMention(ctx context.Context, evt *model.InboxEvent) error {
	var e model.SubscriptionMentionEvent
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal subscription_mention payload: %w", err)
	}
	if len(e.Accounts) == 0 {
		return nil
	}
	if err := h.store.SetSubscriptionMentions(ctx, e.RoomID, e.Accounts, time.UnixMilli(e.MentionedAt).UTC()); err != nil {
		return fmt.Errorf("set subscription mentions in room %q: %w", e.RoomID, err)
	}
	return nil
}
```

- [ ] **Step 5: Implement the Mongo store method**

In `inbox-worker/main.go`, next to the other `mongoInboxStore` subscription methods:

```go
// SetSubscriptionMentions flags the accounts' subscriptions as mentioned. The
// guard is $not/$gte rather than $lt so a never-read subscription (missing
// lastSeenAt) still matches — plain $lt skips missing fields (#467).
func (s *mongoInboxStore) SetSubscriptionMentions(ctx context.Context, roomID string, accounts []string, msgCreatedAt time.Time) error {
	_, err := s.subCol.UpdateMany(ctx,
		bson.M{
			"roomId":     roomID,
			"u.account":  bson.M{"$in": accounts},
			"lastSeenAt": bson.M{"$not": bson.M{"$gte": msgCreatedAt}},
		},
		bson.M{"$set": bson.M{"hasMention": true}},
	)
	if err != nil {
		return fmt.Errorf("set subscription mentions for room %s: %w", roomID, err)
	}
	return nil
}
```

- [ ] **Step 6: Regenerate mocks and run the tests**

Run: `make generate SERVICE=inbox-worker` then `make test SERVICE=inbox-worker`
Expected: PASS.

- [ ] **Step 7: Write the failing integration test**

Append to `inbox-worker/integration_test.go` (the file already carries `//go:build integration` and a `TestMain`). Match the existing per-test store construction at `:33`:

```go
func TestMongoInboxStore_SetSubscriptionMentions(t *testing.T) {
	db := testDB(t)
	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		roomCol:      db.Collection("rooms"),
		userCol:      db.Collection("users"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}
	ctx := context.Background()
	msgAt := time.Now().UTC().Truncate(time.Millisecond)

	// unread: never read at all (no lastSeenAt) — must be badged.
	// stale:  read before the message — must be badged.
	// caught: already read past the message — must NOT be badged.
	// other:  a different room — must NOT be badged.
	_, err := store.subCol.InsertMany(ctx, []any{
		bson.M{"_id": "s1", "roomId": "room-1", "u": bson.M{"account": "unread"}},
		bson.M{"_id": "s2", "roomId": "room-1", "u": bson.M{"account": "stale"}, "lastSeenAt": msgAt.Add(-time.Minute)},
		bson.M{"_id": "s3", "roomId": "room-1", "u": bson.M{"account": "caught"}, "lastSeenAt": msgAt.Add(time.Minute)},
		bson.M{"_id": "s4", "roomId": "room-2", "u": bson.M{"account": "unread"}},
	})
	require.NoError(t, err)

	require.NoError(t, store.SetSubscriptionMentions(ctx, "room-1",
		[]string{"unread", "stale", "caught", "absent"}, msgAt))

	for _, tc := range []struct {
		id   string
		want bool
	}{{"s1", true}, {"s2", true}, {"s3", false}, {"s4", false}} {
		var got struct {
			HasMention bool `bson:"hasMention"`
		}
		require.NoError(t, store.subCol.FindOne(ctx, bson.M{"_id": tc.id}).Decode(&got))
		assert.Equal(t, tc.want, got.HasMention, "subscription %s", tc.id)
	}
}
```

- [ ] **Step 8: Run the integration test**

Run: `make test-integration SERVICE=inbox-worker`
Expected: PASS (Docker must be running).

- [ ] **Step 9: Commit**

```bash
git add inbox-worker/
git commit -m "feat(inbox-worker): apply federated subscription_mention badges"
```

---

### Task 3: Producer — `broadcast-worker` federates new-message mentions

**Files:**
- Modify: `broadcast-worker/handler.go` (`Handler` struct `:64-74`, `handlerOptions` `:78-80`, `NewHandler` `:84-103`, `handleCreated` `:176-237`)
- Modify: `broadcast-worker/main.go` (`NewHandler` call at `:226`)
- Test: `broadcast-worker/handler_test.go`

**Interfaces:**
- Consumes: `model.InboxSubscriptionMention`, `model.SubscriptionMentionEvent` (Task 1); `outbox.Publish` (existing).
- Produces:
  - `type OutboxPublishFunc func(ctx context.Context, subj string, data []byte, msgID string) error`
  - `func withOutboxFederation(siteID string, publish OutboxPublishFunc) handlerOption`
  - `func (h *Handler) federateMentions(ctx context.Context, roomID, msgID, dedupPrefix string, participants []model.Participant, at time.Time)`

- [ ] **Step 1: Write the failing test**

Append to `broadcast-worker/handler_test.go`. `mentionOutboxRecorder` is defined here and reused by Task 4:

```go
// mentionOutboxRecorder captures OUTBOX publishes so tests can assert the
// per-destination fan-out without a real JetStream connection.
type mentionOutboxRecorder struct {
	mu      sync.Mutex
	records []outboxRecord
	err     error
}

type outboxRecord struct {
	subject string
	msgID   string
	event   model.SubscriptionMentionEvent
}

func (r *mentionOutboxRecorder) publish(_ context.Context, subj string, data []byte, msgID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	var outboxEvt model.OutboxEvent
	if err := json.Unmarshal(data, &outboxEvt); err != nil {
		return err
	}
	var envelope model.InboxEvent
	if err := json.Unmarshal(outboxEvt.Envelope, &envelope); err != nil {
		return err
	}
	var evt model.SubscriptionMentionEvent
	if err := json.Unmarshal(envelope.Payload, &evt); err != nil {
		return err
	}
	r.records = append(r.records, outboxRecord{subject: subj, msgID: msgID, event: evt})
	return nil
}

func (r *mentionOutboxRecorder) sorted() []outboxRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := append([]outboxRecord(nil), r.records...)
	sort.Slice(out, func(i, j int) bool { return out[i].subject < out[j].subject })
	return out
}

func TestHandler_HandleCreated_FederatesMentions(t *testing.T) {
	msgTime := time.Now().UTC().Truncate(time.Millisecond)

	tests := []struct {
		name        string
		content     string
		users       []model.User
		publishErr  error
		noFederate  bool
		wantRecords []outboxRecord
	}{
		{
			name:    "all mentionees are local",
			content: "hi @alice",
			users:   []model.User{{ID: "u1", Account: "alice", SiteID: "site-a"}},
		},
		{
			name:    "one event per remote site carrying only that site's accounts",
			content: "hi @alice @bob @carol",
			users: []model.User{
				{ID: "u1", Account: "alice", SiteID: "site-a"},
				{ID: "u2", Account: "bob", SiteID: "site-b"},
				{ID: "u3", Account: "carol", SiteID: "site-c"},
			},
			wantRecords: []outboxRecord{
				{
					subject: "chat.outbox.site-a.site-b.subscription_mention",
					msgID:   testMentionRequestID + ":site-b",
					event: model.SubscriptionMentionEvent{
						RoomID: "room-1", Accounts: []string{"bob"}, MentionedAt: msgTime.UnixMilli(),
					},
				},
				{
					subject: "chat.outbox.site-a.site-c.subscription_mention",
					msgID:   testMentionRequestID + ":site-c",
					event: model.SubscriptionMentionEvent{
						RoomID: "room-1", Accounts: []string{"carol"}, MentionedAt: msgTime.UnixMilli(),
					},
				},
			},
		},
		{
			name:    "unresolved mentionee has no home site to route to",
			content: "hi @ghost",
			users:   nil,
		},
		{
			name:    "mention-all alone federates nothing",
			content: "hi @all",
			users:   nil,
		},
		{
			name:       "publish error is swallowed",
			content:    "hi @bob",
			users:      []model.User{{ID: "u2", Account: "bob", SiteID: "site-b"}},
			publishErr: errors.New("jetstream down"),
		},
		{
			name:       "federation disabled",
			content:    "hi @bob",
			users:      []model.User{{ID: "u2", Account: "bob", SiteID: "site-b"}},
			noFederate: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			us := NewMockUserStore(ctrl)
			pub := &mockPublisher{}
			keyStore := NewMockRoomKeyProvider(ctrl)
			rec := &mentionOutboxRecorder{err: tc.publishErr}

			mentionAll := strings.Contains(tc.content, "@all")
			store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, mentionAll).Return(nil)
			store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
			store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
			us.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(tc.users, nil)
			store.EXPECT().SetSubscriptionMentions(gomock.Any(), "room-1", gomock.Any(), msgTime).Return(nil).AnyTimes()
			keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(testRoomKey(t), nil)

			opts := []handlerOption{withBroadcastMetrics(newBroadcastMetrics(noop.NewMeterProvider().Meter("t")))}
			if !tc.noFederate {
				opts = append(opts, withOutboxFederation("site-a", rec.publish))
			}
			h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal, opts...)

			data, err := json.Marshal(model.MessageEvent{
				Event:  model.EventCreated,
				SiteID: "site-a",
				Message: model.Message{
					ID: "msg-1", RoomID: "room-1", UserID: "user-1", UserAccount: "sender",
					Content: tc.content, CreatedAt: msgTime,
				},
			})
			require.NoError(t, err)

			ctx := natsutil.WithRequestID(context.Background(), testMentionRequestID)
			require.NoError(t, h.HandleMessage(ctx, data))
			assert.Len(t, pub.records, 1, "the client fan-out must still happen")

			got := rec.sorted()
			require.Len(t, got, len(tc.wantRecords))
			for i, want := range tc.wantRecords {
				assert.Equal(t, want.subject, got[i].subject)
				assert.Equal(t, want.msgID, got[i].msgID)
				assert.Equal(t, want.event.RoomID, got[i].event.RoomID)
				assert.Equal(t, want.event.Accounts, got[i].event.Accounts)
				assert.Equal(t, want.event.MentionedAt, got[i].event.MentionedAt)
				assert.NotZero(t, got[i].event.Timestamp)
			}
		})
	}
}
```

Add `sort`, `strings`, `sync`, `errors` and `go.opentelemetry.io/otel/metric/noop` to the test file's imports if not already present.

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=broadcast-worker`
Expected: compile failure — `undefined: withOutboxFederation`.

- [ ] **Step 3: Add the handler dependency**

In `broadcast-worker/handler.go`, above the `Handler` struct:

```go
// OutboxPublishFunc publishes to JetStream with msgID as the Nats-Msg-Id.
// Mirrors message-worker's publish closure so both federate through pkg/outbox.
type OutboxPublishFunc func(ctx context.Context, subj string, data []byte, msgID string) error
```

Add to the `Handler` struct:

```go
	siteID string
	// federate relays mention badges onto the OUTBOX; nil disables the fan-out
	// (tests that don't exercise federation), mirroring inbox-worker's nil badge cache.
	federate OutboxPublishFunc
```

Add to `handlerOptions`:

```go
	siteID   string
	federate OutboxPublishFunc
```

Add the option and wire both fields through `NewHandler` (`siteID: opts.siteID, federate: opts.federate`):

```go
// withOutboxFederation enables the cross-site mention fan-out from siteID.
func withOutboxFederation(siteID string, publish OutboxPublishFunc) handlerOption {
	return func(opts *handlerOptions) {
		opts.siteID = siteID
		opts.federate = publish
	}
}
```

- [ ] **Step 4: Implement the fan-out helper**

In `broadcast-worker/handler.go`, below `badgeNewlyMentionedAccounts`:

```go
// federateMentions relays the mention badge to each mentionee's home site, one
// event per destination carrying only that site's accounts. Best-effort: a
// publish failure is logged, never returned, so it can't NAK the message and
// re-broadcast it to clients. Mentionees the user lookup didn't resolve have no
// known home site and are skipped by the caller (they never become Participants).
func (h *Handler) federateMentions(ctx context.Context, roomID, msgID, dedupPrefix string, participants []model.Participant, at time.Time) {
	if h.federate == nil || len(participants) == 0 {
		return
	}
	accountsBySite := make(map[string][]string)
	for i := range participants {
		p := &participants[i]
		if p.SiteID == "" || p.SiteID == h.siteID {
			continue
		}
		accountsBySite[p.SiteID] = append(accountsBySite[p.SiteID], p.Account)
	}
	now := time.Now().UTC().UnixMilli()
	for destSiteID, accounts := range accountsBySite {
		payload, err := sonic.Marshal(model.SubscriptionMentionEvent{
			RoomID:           roomID,
			Accounts:         accounts,
			MentionedAt: at.UnixMilli(),
			Timestamp:        now,
		})
		if err != nil {
			slog.ErrorContext(ctx, "marshal subscription_mention failed",
				"error", err, "room_id", roomID, "dest_site", destSiteID,
				"request_id", natsutil.RequestIDFromContext(ctx))
			continue
		}
		dedupID := fmt.Sprintf("%s:%s:%s:%s", dedupPrefix, roomID, msgID, destSiteID)
		if err := outbox.Publish(ctx, h.federate, h.siteID, roomID, destSiteID,
			model.InboxSubscriptionMention, payload, dedupID, now); err != nil {
			slog.ErrorContext(ctx, "federate subscription_mention failed",
				"error", err, "room_id", roomID, "dest_site", destSiteID, "accounts", len(accounts),
				"request_id", natsutil.RequestIDFromContext(ctx))
		}
	}
}
```

Add `"github.com/hmchangw/chat/pkg/outbox"` to the imports.

- [ ] **Step 5: Call it from `handleCreated`**

In `broadcast-worker/handler.go`, replace the `switch meta.Type` tail of `handleCreated` so the fan-out runs after the client publish:

```go
	var pubErr error
	switch meta.Type {
	case model.RoomTypeChannel:
		pubErr = h.publishChannelEvent(ctx, &meta, clientMsg, evt.Timestamp, resolved.MentionAll, resolved.Participants)
	case model.RoomTypeDM, model.RoomTypeBotDM:
		pubErr = h.publishDMEvents(ctx, &meta, clientMsg, evt.Timestamp, resolved.Accounts, model.RoomEventNewMessage)
	default:
		slog.WarnContext(ctx, "unknown room type, skipping fan-out",
			"type", meta.Type,
			"room_id", meta.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
	if pubErr != nil {
		return pubErr
	}
	h.federateMentions(ctx, meta.ID, msg.ID, "mention", resolved.Participants, msg.CreatedAt)
	return nil
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS, with every pre-existing test still green (they pass no `withOutboxFederation`, so `federate` is nil and the fan-out no-ops).

- [ ] **Step 7: Wire the publisher in `main.go`**

In `broadcast-worker/main.go`, before the `NewHandler` call at `:226`:

```go
	// JetStream publish for the OUTBOX mention relay; core NATS still carries the
	// client fan-out (natsPublisher above).
	outboxPublish := func(ctx context.Context, subj string, data []byte, msgID string) error {
		_, err := js.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data), jetstream.WithMsgID(msgID))
		publishMetrics.Attempt(ctx, natsmetrics.DestinationOutbox, natsmetrics.OperationOutboxPublish, err)
		if err != nil {
			return fmt.Errorf("publish jetstream message to %s with msgID %s: %w", subj, msgID, err)
		}
		return nil
	}
```

and extend the `NewHandler` call:

```go
	handler := NewHandler(coalescer, us, publisher, keyProvider, parentFetcher, cfg.Encryption.Enabled, roomRouteMode,
		withBroadcastMetrics(domainMetrics), withOutboxFederation(cfg.SiteID, outboxPublish))
```

- [ ] **Step 8: Verify the build and the full unit suite**

Run: `make build SERVICE=broadcast-worker` then `make test SERVICE=broadcast-worker`
Expected: both succeed.

- [ ] **Step 9: Commit**

```bash
git add broadcast-worker/
git commit -m "feat(broadcast-worker): federate new-message mention badges cross-site"
```

---

### Task 4: Producer — `broadcast-worker` federates edited-message mentions

**Files:**
- Modify: `broadcast-worker/handler.go` (`handleUpdated` `:311-342`, `badgeNewlyMentionedAccounts` `:344-354`)
- Test: `broadcast-worker/handler_test.go`

**Interfaces:**
- Consumes: `federateMentions`, `withOutboxFederation`, `mentionOutboxRecorder` (Task 3).
- Produces: nothing for later tasks.

- [ ] **Step 1: Write the failing test**

Append to `broadcast-worker/handler_test.go`:

```go
func TestHandler_HandleUpdated_FederatesMentions(t *testing.T) {
	createdAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	editedAt := time.Now().UTC().Truncate(time.Millisecond)

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)
	rec := &mentionOutboxRecorder{}

	store.EXPECT().GetRoom(gomock.Any(), "room-1").Return(testChannelRoom, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).
		Return([]model.User{{ID: "u2", Account: "bob", SiteID: "site-b"}}, nil)
	store.EXPECT().SetSubscriptionMentions(gomock.Any(), "room-1", []string{"bob"}, editedAt).Return(nil)
	keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(testRoomKey(t), nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal,
		withOutboxFederation("site-a", rec.publish))

	data, err := json.Marshal(model.MessageEvent{
		Event:  model.EventUpdated,
		SiteID: "site-a",
		Message: model.Message{
			ID: "msg-1", RoomID: "room-1", UserID: "user-1", UserAccount: "sender",
			Content: "hi @bob", CreatedAt: createdAt, EditedAt: &editedAt, UpdatedAt: &editedAt,
		},
	})
	require.NoError(t, err)
	ctx := natsutil.WithRequestID(context.Background(), testMentionRequestID)
	require.NoError(t, h.HandleMessage(ctx, data))

	got := rec.sorted()
	require.Len(t, got, 1)
	assert.Equal(t, "chat.outbox.site-a.site-b.subscription_mention", got[0].subject)
	assert.Equal(t, testMentionRequestID+":site-b", got[0].msgID)
	assert.Equal(t, []string{"bob"}, got[0].event.Accounts)
	assert.Equal(t, editedAt.UnixMilli(), got[0].event.MentionedAt)
}

func TestHandler_HandleUpdated_NoMentionsSkipsLookup(t *testing.T) {
	createdAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	editedAt := time.Now().UTC().Truncate(time.Millisecond)

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)
	rec := &mentionOutboxRecorder{}

	store.EXPECT().GetRoom(gomock.Any(), "room-1").Return(testChannelRoom, nil)
	keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(testRoomKey(t), nil)
	// No FindUsersByAccounts and no SetSubscriptionMentions: gomock fails the test if either is called.

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal,
		withOutboxFederation("site-a", rec.publish))

	data, err := json.Marshal(model.MessageEvent{
		Event:  model.EventUpdated,
		SiteID: "site-a",
		Message: model.Message{
			ID: "msg-1", RoomID: "room-1", UserID: "user-1", UserAccount: "sender",
			Content: "no mentions here", CreatedAt: createdAt, EditedAt: &editedAt, UpdatedAt: &editedAt,
		},
	})
	require.NoError(t, err)
	require.NoError(t, h.HandleMessage(context.Background(), data))
	assert.Empty(t, rec.sorted())
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=broadcast-worker -run TestHandler_HandleUpdated_FederatesMentions`
Expected: FAIL — no OUTBOX record was captured (`Len(got, 1)` fails), because the edit path does not federate yet.

- [ ] **Step 3: Resolve mentionees to sites on the edit path**

In `broadcast-worker/handler.go`, replace `badgeNewlyMentionedAccounts` with a version that returns the resolved participants:

```go
// badgeNewlyMentionedAccounts badges the accounts an edit @-mentions, mirroring
// handleCreated. Additive only: SetSubscriptionMentions' filter skips
// non-subscribers and accounts that have already read past the edit, so a
// removed mention is never cleared and an already-read one is never re-flagged.
// Returns the resolved mentionees so the caller can federate them; a lookup
// failure still badges locally, it only costs the cross-site relay.
func (h *Handler) badgeNewlyMentionedAccounts(ctx context.Context, roomID string, msg *model.Message) ([]model.Participant, error) {
	parsed := mention.Parse(msg.Content)
	if len(parsed.Accounts) == 0 {
		return nil, nil
	}
	users, err := h.userStore.FindUsersByAccounts(ctx, parsed.Accounts)
	if err != nil {
		slog.WarnContext(ctx, "user lookup failed for edited mentions, skipping federation",
			"error", err, "room_id", roomID,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
	if err := h.store.SetSubscriptionMentions(ctx, roomID, parsed.Accounts, *msg.EditedAt); err != nil {
		return nil, err
	}
	return mention.ResolveFromParsed(parsed, usersByAccount(users)).Participants, nil
}
```

- [ ] **Step 4: Federate from `handleUpdated`**

In `broadcast-worker/handler.go`, in `handleUpdated`, replace the `badgeNewlyMentionedAccounts` call and the trailing publish:

```go
	mentioned, err := h.badgeNewlyMentionedAccounts(ctx, room.ID, &msg)
	if err != nil {
		return fmt.Errorf("badge new mentions on edit %s: %w", room.ID, err)
	}

	edit := buildEditRoomEvent(room, evt)
	if room.Type == model.RoomTypeChannel && h.encrypt {
		if err := h.encryptEditedContent(ctx, room.ID, &edit); err != nil {
			return fmt.Errorf("encrypt edit content for room %s: %w", room.ID, err)
		}
	}
	if err := h.publishMutation(ctx, room, model.RoomEventMessageEdited, msg.ID, &edit); err != nil {
		return err
	}
	// editedAt is in the dedup seed so a later edit adding a new mention isn't
	// swallowed by stream-level dedup.
	h.federateMentions(ctx, room.ID, fmt.Sprintf("%s:%d", msg.ID, msg.EditedAt.UnixMilli()),
		"mention-edit", mentioned, *msg.EditedAt)
	return nil
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS, including the pre-existing edit tests.

- [ ] **Step 6: Commit**

```bash
git add broadcast-worker/
git commit -m "feat(broadcast-worker): federate edited-message mention badges cross-site"
```

---

### Task 5: End-to-end verification and repo gates

**Files:**
- Modify: none expected. Fix whatever the gates flag.

**Interfaces:**
- Consumes: everything from Tasks 1-4.
- Produces: a branch that passes lint, unit tests, integration tests and SAST.

- [ ] **Step 1: Regenerate all mocks**

Run: `make generate`
Expected: no unexpected diff beyond `inbox-worker/mock_store_test.go`.

- [ ] **Step 2: Format and lint**

Run: `make fmt` then `make lint`
Expected: clean.

- [ ] **Step 3: Full unit suite with the race detector**

Run: `make test`
Expected: PASS.

- [ ] **Step 4: Coverage check on the changed packages**

Run: `go test -coverprofile=/tmp/cover.out ./broadcast-worker/ ./inbox-worker/ ./pkg/outbox/ && go tool cover -func=/tmp/cover.out | tail -1`
Expected: the new functions (`federateMentions`, `handleSubscriptionMention`, `SetSubscriptionMentions`) are covered and total coverage does not drop below 80%.

- [ ] **Step 5: Integration tests for the touched services**

Run: `make test-integration SERVICE=inbox-worker` then `make test-integration SERVICE=broadcast-worker`
Expected: PASS. Requires a running Docker daemon.

- [ ] **Step 6: SAST**

Run: `make sast`
Expected: no medium-or-higher findings.

- [ ] **Step 7: Commit any fixes**

```bash
git add -A
git commit -m "chore: lint and mock regeneration for mention fan-out"
```
