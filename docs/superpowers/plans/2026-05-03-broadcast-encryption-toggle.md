# Broadcast-Worker Encryption Toggle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `ENCRYPTION_ENABLED` env var (default `false`) to `broadcast-worker` that gates both the channel-message encryption code path and the Valkey connection at startup.

**Architecture:** New `encryptionConfig` sub-struct with `envPrefix:"ENCRYPTION_"` in `config`. `main.go` only constructs `keyStore` when `Enabled=true`. `Handler` gains an `encrypt bool` field; `publishChannelEvent` branches on it — encrypted path unchanged, plaintext path mirrors the DM behavior (`evt.Message = clientMsg`). DM path is untouched.

**Tech Stack:** Go 1.25, `caarlos0/env/v11`, `go.uber.org/mock`, `stretchr/testify`, `testcontainers-go`.

**Spec:** `docs/superpowers/specs/2026-05-03-broadcast-encryption-toggle-design.md`

**Branch:** `claude/broadcast-encryption-toggle-ZGc4m`

---

## File Structure

Modify only:
- `broadcast-worker/handler.go` — add `encrypt` field, branch in `publishChannelEvent`.
- `broadcast-worker/handler_test.go` — pass `true` to existing `NewHandler` calls; add new disabled-encryption tests.
- `broadcast-worker/integration_test.go` — pass `true` to existing `NewHandler` calls; add one plaintext-channel test.
- `broadcast-worker/main.go` — add `encryptionConfig`, relax `,required` on Valkey vars, conditional Valkey wiring, conditional shutdown hook, log encryption flag at startup, pass flag to `NewHandler`.
- `broadcast-worker/deploy/docker-compose.yml` — add `ENCRYPTION_ENABLED=false`.

No new files. No changes to other services or `pkg/`.

---

## Task 1: Handler — encrypt field and plaintext branch (TDD)

**Files:**
- Modify: `broadcast-worker/handler.go` (struct around lines 35-44, `publishChannelEvent` around lines 94-133)
- Modify: `broadcast-worker/handler_test.go` (every `NewHandler(store, us, pub, keyStore)` call site; add new test `TestHandler_HandleMessage_ChannelEncryptionDisabled`)
- Modify: `broadcast-worker/integration_test.go` (every `NewHandler(...)` call site at lines 85, 130, 169, 218 — see Task 3 for new integration test, that's a separate commit)

- [ ] **Step 1.1: Write the failing unit test**

Append to `broadcast-worker/handler_test.go` at the end of the file (after `TestBuildClientMessage` or wherever the file ends):

```go
func TestHandler_HandleMessage_ChannelEncryptionDisabled(t *testing.T) {
	msgTime := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	senderUser := model.User{ID: "u-sender", Account: "sender", EngName: "Sender Lin", ChineseName: "寄件者", SiteID: "site-a"}

	tests := []struct {
		name            string
		content         string
		wantMentionAll  bool
		wantMentions    []string
		wantSetMentions []string
	}{
		{
			name:           "plaintext no mentions",
			content:        "hello group",
			wantMentionAll: false,
		},
		{
			name:            "plaintext individual mention",
			content:         "hey @alice",
			wantMentions:    []string{"alice"},
			wantSetMentions: []string{"alice"},
		},
		{
			name:           "plaintext mention all",
			content:        "attention @all",
			wantMentionAll: true,
			wantMentions:   []string{"all"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			us := NewMockUserStore(ctrl)
			pub := &mockPublisher{}

			store.EXPECT().FetchAndUpdateRoom(gomock.Any(), "room-1", "msg-1", msgTime, tc.wantMentionAll).Return(testChannelRoom, nil)
			if tc.wantSetMentions != nil {
				store.EXPECT().SetSubscriptionMentions(gomock.Any(), "room-1", gomock.InAnyOrder(tc.wantSetMentions)).Return(nil)
			}

			switch tc.name {
			case "plaintext individual mention":
				us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil)
			}
			us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return([]model.User{senderUser}, nil)

			// nil keyStore — handler must NOT dereference it when encrypt=false
			h := NewHandler(store, us, pub, nil, false)
			err := h.HandleMessage(context.Background(), makeMessageEvent("room-1", tc.content, msgTime))
			require.NoError(t, err)

			require.Len(t, pub.records, 1)
			assert.Equal(t, subject.RoomEvent("room-1"), pub.records[0].subject)

			var evt model.RoomEvent
			require.NoError(t, json.Unmarshal(pub.records[0].data, &evt))
			require.NotNil(t, evt.Message, "plaintext channel events must carry Message")
			assert.Empty(t, evt.EncryptedMessage, "plaintext channel events must NOT carry EncryptedMessage")
			assert.Equal(t, "msg-1", evt.Message.ID)
			require.NotNil(t, evt.Message.Sender)
			assert.Equal(t, "sender", evt.Message.Sender.Account)
			assert.Equal(t, tc.wantMentionAll, evt.MentionAll)

			if tc.wantMentions != nil {
				accounts := make([]string, len(evt.Mentions))
				for i, m := range evt.Mentions {
					accounts[i] = m.Account
				}
				assert.ElementsMatch(t, tc.wantMentions, accounts)
			} else {
				assert.Empty(t, evt.Mentions)
			}
		})
	}
}
```

- [ ] **Step 1.2: Run the new test to verify it fails to compile**

Run: `make test SERVICE=broadcast-worker`
Expected: compile error — `too many arguments in call to NewHandler` (or similar). The new test calls `NewHandler` with 5 args; current signature has 4.

- [ ] **Step 1.3: Update `Handler` struct and `NewHandler`**

In `broadcast-worker/handler.go`, replace the struct + constructor:

```go
// Handler processes MESSAGES_CANONICAL messages and broadcasts room events.
type Handler struct {
	store     Store
	userStore userstore.UserStore
	pub       Publisher
	keyStore  RoomKeyProvider
	encrypt   bool
}

func NewHandler(store Store, userStore userstore.UserStore, pub Publisher, keyStore RoomKeyProvider, encrypt bool) *Handler {
	return &Handler{store: store, userStore: userStore, pub: pub, keyStore: keyStore, encrypt: encrypt}
}
```

- [ ] **Step 1.4: Update existing call sites in `handler_test.go`**

Every existing `NewHandler(store, us, pub, keyStore)` call in `broadcast-worker/handler_test.go` becomes `NewHandler(store, us, pub, keyStore, true)`. The call sites are at (current) lines 136, 233, 273, 289, 304, 321, 342, 359, 383+390, 410+415, 456, 486, 505, 526.

Use a single sed-style replacement OR manually edit each. The mechanical replacement:

```bash
cd /home/user/chat
# Inspect first to confirm pattern is unique enough
grep -n "NewHandler(store, us, pub, keyStore)" broadcast-worker/handler_test.go
# Then apply
sed -i 's/NewHandler(store, us, pub, keyStore)/NewHandler(store, us, pub, keyStore, true)/g' broadcast-worker/handler_test.go
# Verify
grep -n "NewHandler(" broadcast-worker/handler_test.go
```

Expected after sed: every line either calls `NewHandler(store, us, pub, keyStore, true)` or the new test's `NewHandler(store, us, pub, nil, false)` from Step 1.1.

- [ ] **Step 1.5: Update existing call sites in `integration_test.go`**

In `broadcast-worker/integration_test.go`, lines 85, 130, 169, 218:

```bash
sed -i 's/NewHandler(store, us, pub, keyStore)/NewHandler(store, us, pub, keyStore, true)/g' broadcast-worker/integration_test.go
grep -n "NewHandler(" broadcast-worker/integration_test.go
```

Expected: 4 matches, all with the trailing `, true`.

- [ ] **Step 1.6: Run unit tests — new test should now FAIL at runtime, others should pass**

Run: `make test SERVICE=broadcast-worker`
Expected: compile passes. `TestHandler_HandleMessage_ChannelEncryptionDisabled` fails — most likely with a nil-pointer panic (handler still dereferences `h.keyStore`) or with assertion failure (`evt.Message` is nil because the encrypted branch zeroed it). All other tests pass.

- [ ] **Step 1.7: Add the plaintext branch in `publishChannelEvent`**

In `broadcast-worker/handler.go`, replace the body of `publishChannelEvent` (current lines 94-133):

```go
func (h *Handler) publishChannelEvent(ctx context.Context, room *model.Room, clientMsg *model.ClientMessage, mentionAll bool, mentions []model.Participant) error {
	evt := buildRoomEvent(room, clientMsg)
	evt.MentionAll = mentionAll
	if len(mentions) > 0 {
		evt.Mentions = mentions
	}

	if h.encrypt {
		msgJSON, err := json.Marshal(clientMsg)
		if err != nil {
			return fmt.Errorf("marshal client message: %w", err)
		}

		key, err := h.keyStore.Get(ctx, room.ID)
		if err != nil {
			return fmt.Errorf("get room key for room %s: %w", room.ID, err)
		}
		if key == nil {
			return fmt.Errorf("get room key for room %s: %w", room.ID, errNoCurrentKey)
		}

		encrypted, err := roomcrypto.Encode(string(msgJSON), key.KeyPair.PublicKey, key.Version)
		if err != nil {
			return fmt.Errorf("encrypt message for room %s: %w", room.ID, err)
		}

		encJSON, err := json.Marshal(encrypted)
		if err != nil {
			return fmt.Errorf("marshal encrypted message: %w", err)
		}

		evt.EncryptedMessage = json.RawMessage(encJSON)
		evt.Message = nil
	}
	// when h.encrypt is false, evt.Message is already set by buildRoomEvent

	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal channel event: %w", err)
	}

	return h.pub.Publish(ctx, subject.RoomEvent(room.ID), payload)
}
```

- [ ] **Step 1.8: Run unit tests — all should pass**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS. All tests including the three new `TestHandler_HandleMessage_ChannelEncryptionDisabled` subtests.

- [ ] **Step 1.9: Run lint**

Run: `make lint`
Expected: no findings on broadcast-worker.

- [ ] **Step 1.10: Commit**

```bash
git add broadcast-worker/handler.go broadcast-worker/handler_test.go broadcast-worker/integration_test.go
git commit -m "$(cat <<'EOF'
feat(broadcast-worker): add encrypt flag to Handler

Introduce an `encrypt bool` field on Handler. When false, publishChannelEvent
keeps the plaintext Message on the RoomEvent and does not consult keyStore;
when true, the existing Valkey-key + roomcrypto.Encode path is used. Existing
call sites pass true to preserve current behavior; main.go wiring lands in the
next commit.

Spec: docs/superpowers/specs/2026-05-03-broadcast-encryption-toggle-design.md
EOF
)"
```

---

## Task 2: main.go wiring + docker-compose

**Files:**
- Modify: `broadcast-worker/main.go` (config struct lines 26-41, Valkey wiring lines 75-83, NewHandler call line 114, shutdown hooks line 172, startup log line 153)
- Modify: `broadcast-worker/deploy/docker-compose.yml`

- [ ] **Step 2.1: Add `encryptionConfig` and field, relax `,required` on Valkey vars**

In `broadcast-worker/main.go`, update the config block. Replace the existing `config` struct (current lines 26-41) with:

```go
type encryptionConfig struct {
	Enabled bool `env:"ENABLED" envDefault:"false"`
}

type config struct {
	NatsURL              string           `env:"NATS_URL"                  envDefault:"nats://localhost:4222"`
	NatsCredsFile        string           `env:"NATS_CREDS_FILE"           envDefault:""`
	SiteID               string           `env:"SITE_ID"                   envDefault:"default"`
	MongoURI             string           `env:"MONGO_URI"                 envDefault:"mongodb://localhost:27017"`
	MongoDB              string           `env:"MONGO_DB"                  envDefault:"chat"`
	MongoUsername        string           `env:"MONGO_USERNAME"            envDefault:""`
	MongoPassword        string           `env:"MONGO_PASSWORD"            envDefault:""`
	MaxWorkers           int              `env:"MAX_WORKERS"               envDefault:"100"`
	UserCacheSize        int              `env:"USER_CACHE_SIZE"           envDefault:"10000"`
	UserCacheTTL         time.Duration    `env:"USER_CACHE_TTL"            envDefault:"5m"`
	ValkeyAddr           string           `env:"VALKEY_ADDR"`
	ValkeyPassword       string           `env:"VALKEY_PASSWORD"           envDefault:""`
	ValkeyKeyGracePeriod time.Duration    `env:"VALKEY_KEY_GRACE_PERIOD"`
	Bootstrap            bootstrapConfig  `envPrefix:"BOOTSTRAP_"`
	Encryption           encryptionConfig `envPrefix:"ENCRYPTION_"`
}
```

Note: `VALKEY_ADDR,required` becomes `VALKEY_ADDR`; `VALKEY_KEY_GRACE_PERIOD,required` becomes `VALKEY_KEY_GRACE_PERIOD`. Validation moves into the conditional wiring block in Step 2.2.

- [ ] **Step 2.2: Replace Valkey wiring with conditional block**

In `broadcast-worker/main.go`, replace the existing Valkey block (current lines 75-83):

```go
	keyStore, err := roomkeystore.NewValkeyStore(roomkeystore.Config{
		Addr:        cfg.ValkeyAddr,
		Password:    cfg.ValkeyPassword,
		GracePeriod: cfg.ValkeyKeyGracePeriod,
	})
	if err != nil {
		slog.Error("valkey connect failed", "error", err)
		os.Exit(1)
	}
```

with:

```go
	var keyStore *roomkeystore.ValkeyStore
	if cfg.Encryption.Enabled {
		if cfg.ValkeyAddr == "" || cfg.ValkeyKeyGracePeriod <= 0 {
			slog.Error("encryption enabled but VALKEY_ADDR / VALKEY_KEY_GRACE_PERIOD missing")
			os.Exit(1)
		}
		keyStore, err = roomkeystore.NewValkeyStore(roomkeystore.Config{
			Addr:        cfg.ValkeyAddr,
			Password:    cfg.ValkeyPassword,
			GracePeriod: cfg.ValkeyKeyGracePeriod,
		})
		if err != nil {
			slog.Error("valkey connect failed", "error", err)
			os.Exit(1)
		}
	}
```

- [ ] **Step 2.3: Pass the encryption flag to `NewHandler`**

In `broadcast-worker/main.go`, find:

```go
	handler := NewHandler(store, us, publisher, keyStore)
```

Replace with:

```go
	handler := NewHandler(store, us, publisher, keyStore, cfg.Encryption.Enabled)
```

- [ ] **Step 2.4: Add `encryption` field to startup log**

In `broadcast-worker/main.go`, find:

```go
	slog.Info("broadcast-worker started", "site", cfg.SiteID)
```

Replace with:

```go
	slog.Info("broadcast-worker started", "site", cfg.SiteID, "encryption", cfg.Encryption.Enabled)
```

- [ ] **Step 2.5: Make the keyStore shutdown hook conditional**

In `broadcast-worker/main.go`, the `shutdown.Wait` call currently registers `keyStore.Close()` unconditionally. Find:

```go
		func(ctx context.Context) error { return keyStore.Close() },
```

Replace it (and restructure the variadic args) so the hook is only added when `keyStore != nil`. The simplest, most readable approach is to build the hook slice and pass it via `...`:

`pkg/shutdown.Wait` is declared as `func Wait(ctx, timeout, shutdownFuncs ...func(context.Context) error)` — no exported `Hook` alias — so the slice element type is the function literal.

Replace the entire `shutdown.Wait(ctx, 25*time.Second, ...)` call block with:

```go
	hooks := []func(context.Context) error{
		func(ctx context.Context) error {
			iter.Stop()
			return nil
		},
		func(ctx context.Context) error {
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("worker drain timed out: %w", ctx.Err())
			}
		},
		func(ctx context.Context) error { return tracerShutdown(ctx) },
		func(ctx context.Context) error { return nc.Drain() },
	}
	if keyStore != nil {
		hooks = append(hooks, func(ctx context.Context) error { return keyStore.Close() })
	}
	hooks = append(hooks, func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil })

	shutdown.Wait(ctx, 25*time.Second, hooks...)
```

- [ ] **Step 2.6: Build to verify compilation**

Run: `make build SERVICE=broadcast-worker`
Expected: builds cleanly, no errors.

- [ ] **Step 2.7: Update `broadcast-worker/deploy/docker-compose.yml`**

In `broadcast-worker/deploy/docker-compose.yml`, find the `environment:` block (around lines 15-27). Add `ENCRYPTION_ENABLED=false` so dev defaults match production. Replace:

```yaml
      - VALKEY_ADDR=valkey:6379
      - VALKEY_KEY_GRACE_PERIOD=24h
      - BOOTSTRAP_STREAMS=true
```

with:

```yaml
      - VALKEY_ADDR=valkey:6379
      - VALKEY_KEY_GRACE_PERIOD=24h
      - BOOTSTRAP_STREAMS=true
      - ENCRYPTION_ENABLED=false
```

(Keep `VALKEY_*` and the `valkey` service in compose so devs who flip the flag still work locally.)

- [ ] **Step 2.8: Run unit tests, lint**

Run: `make test SERVICE=broadcast-worker && make lint`
Expected: PASS, no findings.

- [ ] **Step 2.9: Commit**

```bash
git add broadcast-worker/main.go broadcast-worker/deploy/docker-compose.yml
git commit -m "$(cat <<'EOF'
feat(broadcast-worker): gate encryption + Valkey behind ENCRYPTION_ENABLED

Add encryptionConfig (envPrefix ENCRYPTION_) with a single Enabled bool, default
false. When disabled, broadcast-worker no longer connects to Valkey and skips
the close hook on shutdown; the handler publishes plaintext channel events.
When enabled, behavior is unchanged; VALKEY_ADDR / VALKEY_KEY_GRACE_PERIOD are
validated at startup. docker-compose.yml sets ENCRYPTION_ENABLED=false so dev
defaults match prod.

Spec: docs/superpowers/specs/2026-05-03-broadcast-encryption-toggle-design.md
EOF
)"
```

---

## Task 3: Integration test for plaintext channel publish

**Files:**
- Modify: `broadcast-worker/integration_test.go` (add `TestBroadcastWorker_ChannelRoom_EncryptionDisabled_Integration`)

- [ ] **Step 3.1: Append the new integration test**

Append to `broadcast-worker/integration_test.go`:

```go
func TestBroadcastWorker_ChannelRoom_EncryptionDisabled_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "rNoEnc", Name: "plain", Type: model.RoomTypeChannel, UserCount: 2, SiteID: "site-a",
	})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "sN1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "rNoEnc"},
		model.Subscription{ID: "sN2", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "rNoEnc"},
	})
	require.NoError(t, err)
	seedUsers(t, db)

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"))
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}

	// nil keyStore — encryption is disabled, handler must not consult it
	handler := NewHandler(store, us, pub, nil, false)

	msgTime := time.Now().UTC().Truncate(time.Millisecond)
	evt := model.MessageEvent{
		SiteID: "site-a",
		Message: model.Message{
			ID: "mNoEnc", RoomID: "rNoEnc", UserID: "u1", UserAccount: "alice", Content: "plaintext please", CreatedAt: msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	require.NoError(t, handler.HandleMessage(ctx, data))

	records := pub.getRecords()
	require.Len(t, records, 1)
	assert.Equal(t, subject.RoomEvent("rNoEnc"), records[0].subject)

	var roomEvt model.RoomEvent
	require.NoError(t, json.Unmarshal(records[0].data, &roomEvt))
	assert.Equal(t, "site-a", roomEvt.SiteID)
	require.NotNil(t, roomEvt.Message, "plaintext channel event must carry Message")
	assert.Empty(t, roomEvt.EncryptedMessage, "plaintext channel event must NOT carry EncryptedMessage")
	assert.Equal(t, "mNoEnc", roomEvt.Message.ID)
	assert.Equal(t, "plaintext please", roomEvt.Message.Content)
	require.NotNil(t, roomEvt.Message.Sender)
	assert.Equal(t, "u1", roomEvt.Message.Sender.UserID)
	assert.Equal(t, "alice", roomEvt.Message.Sender.Account)

	var room model.Room
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "rNoEnc"}).Decode(&room))
	assert.Equal(t, "mNoEnc", room.LastMsgID)
}
```

- [ ] **Step 3.2: Run integration tests**

Run: `make test-integration SERVICE=broadcast-worker`
Expected: PASS, including the new `TestBroadcastWorker_ChannelRoom_EncryptionDisabled_Integration` test. The pre-existing integration tests still pass (they pass `keyStore` and `true`).

- [ ] **Step 3.3: Commit**

```bash
git add broadcast-worker/integration_test.go
git commit -m "$(cat <<'EOF'
test(broadcast-worker): integration test for plaintext channel publish

Adds TestBroadcastWorker_ChannelRoom_EncryptionDisabled_Integration which
constructs the handler with encrypt=false and a nil keyStore, publishes a
canonical channel message, and asserts the RoomEvent carries Message (not
EncryptedMessage) and the room metadata still updates in Mongo.

Spec: docs/superpowers/specs/2026-05-03-broadcast-encryption-toggle-design.md
EOF
)"
```

---

## Task 4: Push the branch

- [ ] **Step 4.1: Push**

Run: `git push -u origin claude/broadcast-encryption-toggle-ZGc4m`
Expected: branch updates remote with three new commits (plus the spec commit already pushed).

---

## Self-Review (against the spec)

- **Spec coverage:**
  - Configuration → Task 2 (`encryptionConfig`, `,required` removal, validation block).
  - Handler change → Task 1 (`encrypt` field, branch in `publishChannelEvent`).
  - Local development (compose default off) → Task 2 Step 2.7.
  - Existing test updates → Task 1 Steps 1.4, 1.5.
  - New unit test (3 sub-cases) → Task 1 Step 1.1.
  - New integration test → Task 3.
  - Conditional shutdown hook → Task 2 Step 2.5.
  - Startup log change → Task 2 Step 2.4.
  - "Out of scope" items (DM, per-room toggle, frontend, metrics) → not in any task. Correct.
- **Placeholder scan:** none — every step has concrete code/commands and expected output.
- **Type consistency:** `Handler.encrypt`, `NewHandler(..., encrypt bool)`, env tag `ENABLED` under prefix `ENCRYPTION_` → env var `ENCRYPTION_ENABLED` — consistent across spec, tasks, and compose.
