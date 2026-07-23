# Bot pipeline unification implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse three parallel bot fan-out services into env-parameterized single-binary two-deployment counterparts of the user pipeline, and fill the room-key lifecycle gap in bot-room-service so unified broadcast can encrypt bot messages.

**Architecture:** For each of `broadcast-worker`, `notification-worker`, `push-notification-service`: one binary reads `INPUT_STREAM` / `INPUT_SUBJECT_FILTER` / `CONSUMER_NAME` (+ `OUTPUT_STREAM` for notification-worker) from env, wires one JS consumer, and is deployed twice (user + bot) with different env overrides — same image. Delete `bot-broadcast-worker/`, `bot-notification-worker/`; rename `bot-push-notification-service/` → `push-notification-service/`. Bot pipeline adopts `model.PushNotificationEvent` verbatim; delete `model.BotNotification`. Bot-room-service gains room-key generation on create/DM-ensure, fan-out on add-member, rotation on remove-member.

**Tech Stack:** Go 1.25, `github.com/nats-io/nats.go/jetstream`, `caarlos0/env`, `pkg/roomkeystore`, `pkg/roomkeysender`, `go.uber.org/mock`, `stretchr/testify`, `testcontainers-go`.

**Spec:** `docs/superpowers/specs/2026-07-22-bot-pipeline-unification-design.md`

**Branch:** `claude/bot-implementation-8d2ctl` (PR #109)

---

## File map

**Modify:**
- `bot-room-service/main.go` — inject `keyStore` (Mongo) and `keySender` (WS publisher)
- `bot-room-service/handler.go` — add `keyStore` + `keySender` fields; wire lifecycle calls in create/DM-ensure/add/remove
- `bot-room-service/handler_test.go` — new tests for key lifecycle
- `bot-room-service/mock_store_test.go` — regenerate
- `broadcast-worker/main.go` — replace hardcoded stream/consumer names with env-driven config
- `broadcast-worker/main_test.go` (or new `config_test.go`) — verify env wiring
- `notification-worker/main.go` — env-driven INPUT + OUTPUT stream/subject/consumer
- `notification-worker/main_test.go` — verify env wiring
- `bot-push-notification-service/*` → **rename** dir to `push-notification-service/`; parameterize main.go on env
- Root `docker-compose.yml` — spin up both deployments of each unified service for local dev
- `docs/superpowers/specs/2026-07-22-bot-cross-site-routing-design.md` — reflect unified architecture

**Create:**
- `broadcast-worker/deploy/user/{Dockerfile, docker-compose.yml, azure-pipelines.yml}` — move existing files here
- `broadcast-worker/deploy/bot/{Dockerfile, docker-compose.yml, azure-pipelines.yml}` — bot env
- `notification-worker/deploy/user/…`, `notification-worker/deploy/bot/…`
- `push-notification-service/deploy/user/…`, `push-notification-service/deploy/bot/…`
- `bot-room-service/keys.go` — thin helpers for key lifecycle (or inline in handler.go if trivial)
- `bot-room-service/keys_test.go` — tests for the helpers

**Delete:**
- `bot-broadcast-worker/` — entire directory
- `bot-notification-worker/` — entire directory
- `pkg/model/bot.go` — the `BotNotification` type (keep other bot types like `BotIdentity`)

---

## Task 1: Bot-room-service — room key on CreateChannel

**Files:**
- Modify: `bot-room-service/handler.go` (add `keyStore roomkeystore.RoomKeyStore`, `keySender *roomkeysender.Sender` fields; wire in `CreateChannel`)
- Modify: `bot-room-service/main.go` (inject keyStore + keySender)
- Modify: `bot-room-service/handler_test.go` (mocked keyStore + keySender assertions)
- Regenerate: `bot-room-service/mock_store_test.go` if `RoomStore` interface changes (it shouldn't)

- [ ] **Step 1: Read reference impl** — `room-service/handler.go` `handleRoomCreate` (locate via `grep -n "GenerateKeyPair\|keyStore.Set" room-service/handler.go`). Understand argument order (`keyStore.Set(ctx, roomID, RoomKeyPair)` returns `(int, error)`) and how the initial owner receives the key (`roomkeysender.Send`).

- [ ] **Step 2: Write failing test in `bot-room-service/handler_test.go`**

```go
func TestHandler_CreateChannel_GeneratesAndFansOutRoomKey(t *testing.T) {
    ctrl := gomock.NewController(t)
    mockStore := NewMockRoomStore(ctrl)
    mockKeyStore := NewMockRoomKeyStore(ctrl)
    mockSender := &captureSender{}

    h := &handler{
        store:     mockStore,
        keyStore:  mockKeyStore,
        keySender: mockSender,
        siteID:    "site-a",
        now:       func() time.Time { return fixedTime },
        newMsgID:  func() string { return "msg-1" },
    }
    mockStore.EXPECT().FindUser(gomock.Any(), "bot-1").Return(botUser, nil)
    mockStore.EXPECT().InsertRoom(gomock.Any(), gomock.Any()).Return(nil)
    mockStore.EXPECT().UpsertSubscription(gomock.Any(), gomock.Any()).Return(true, nil)
    mockKeyStore.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).Return(1, nil)

    resp, err := h.CreateChannel(ctx, req)
    require.NoError(t, err)
    require.Equal(t, 1, len(mockSender.sent), "owner receives one key event")
    require.Equal(t, "bot-1.account", mockSender.sent[0].account)
}
```

- [ ] **Step 3: Run test, confirm FAIL** (handler doesn't have keyStore/keySender fields yet).
  Run: `make test SERVICE=bot-room-service 2>&1 | tail -20`

- [ ] **Step 4: Add fields + Mockgen directive**

```go
// bot-room-service/handler.go — top-level fields
type handler struct {
    // existing…
    keyStore  roomkeystore.RoomKeyStore
    keySender KeySender // narrow interface for testability
}

type KeySender interface {
    Send(account string, evt model.RoomKeyEvent) error
}

// Wire in CreateChannel after InsertRoom + owner subscription upsert:
pair, err := roomkeystore.GenerateKeyPair()
if err != nil {
    return nil, fmt.Errorf("generate room key: %w", err)
}
ver, err := h.keyStore.Set(ctx, room.ID, *pair)
if err != nil {
    return nil, fmt.Errorf("store room key: %w", err)
}
versioned := &roomkeystore.VersionedKeyPair{Version: ver, KeyPair: *pair}
if err := h.keySender.Send(owner.Account, model.RoomKeyEvent{RoomID: room.ID, KeyPair: versioned}); err != nil {
    slog.WarnContext(ctx, "fan out room key to owner failed", "error", err, "roomID", room.ID)
}
```

- [ ] **Step 5: Wire in main.go**

```go
keyStore := roomkeystore.NewMongoStore(db.Collection("rooms"), cfg.RoomKeyGracePeriod)
keySender := roomkeysender.NewSender(natsPublisher(nc))
h := &handler{ /* existing */, keyStore: keyStore, keySender: keySender }
```

Add `RoomKeyGracePeriod time.Duration` to Config with `env:"ROOM_KEY_GRACE_PERIOD" envDefault:"1h"`.

- [ ] **Step 6: Regenerate mocks** — `make generate SERVICE=bot-room-service`. Add `//go:generate mockgen … RoomKeyStore,KeySender` to `store.go` next to existing directive.

- [ ] **Step 7: Run tests, confirm PASS** — `make test SERVICE=bot-room-service`. Fix any compile errors.

- [ ] **Step 8: Commit**

```bash
git add bot-room-service/ 
git commit -m "feat(bot-room-service): generate + fan out room key on CreateChannel"
```

---

## Task 2: SKIPPED

Discovered during Task 1 review: `room-worker/handler.go:1447-1449` documents that DM/botDM rooms are never encrypted (per-user fan-out subjects, no shared room subject, no key stored). Bot pipeline matches. `EnsureDM` needs no key work.

---

## Task 3: Bot-room-service — key fan-out on AddMember

**Files:**
- Modify: `bot-room-service/handler.go` — `AddMember` handler

- [ ] **Step 1: Write failing test** — mock keyStore.Get returns a pair; assert keySender.Send called once per newly-subscribed account (not for accounts whose UpsertSubscription returned created=false).

- [ ] **Step 2: Run, confirm FAIL**.

- [ ] **Step 3: Implement** — after the add-member loop, collect newly-added accounts (where `created=true`), then:

```go
if len(newAccounts) > 0 {
    pair, err := h.keyStore.Get(ctx, req.RoomID)
    if err != nil {
        if errors.Is(err, roomkeystore.ErrNoCurrentKey) {
            slog.WarnContext(ctx, "no current key on add-member; skip fan-out", "roomID", req.RoomID)
        } else {
            return nil, fmt.Errorf("get room key: %w", err)
        }
    } else {
        evt := model.RoomKeyEvent{RoomID: req.RoomID, KeyPair: pair}
        for _, acct := range newAccounts {
            if err := h.keySender.Send(acct, evt); err != nil {
                slog.WarnContext(ctx, "fan out room key on add failed", "account", acct, "error", err)
            }
        }
    }
}
```

- [ ] **Step 4: Run tests, confirm PASS**.

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(bot-room-service): fan out current room key to newly added members"
```

---

## Task 4: Bot-room-service — key rotation on RemoveMember

**Files:**
- Modify: `bot-room-service/handler.go` — `RemoveMember` handler
- Modify: `bot-room-service/store.go` — add a helper if survivor listing needs a projection

- [ ] **Step 1: Reference impl** — read `room-worker/handler.go` `rotateAndFanOut` (already read in brainstorming). Understand `Rotate` vs `SetWithVersion` fallback semantics.

- [ ] **Step 2: Write failing test** — mock keyStore.Get returns v3; mock keyStore.Rotate accepts v4; assert keySender.Send called once per survivor with v4 BEFORE Rotate; assert Rotate called last.

- [ ] **Step 3: Run, confirm FAIL**.

- [ ] **Step 4: Implement** — after DeleteSubscription succeeds and diff is non-empty:

```go
survivors, err := h.store.ListRoomMemberAccounts(ctx, req.RoomID)
if err != nil { return nil, fmt.Errorf("list survivors: %w", err) }

currentPair, err := h.keyStore.Get(ctx, req.RoomID)
if err != nil && !errors.Is(err, roomkeystore.ErrNoCurrentKey) {
    return nil, fmt.Errorf("get current key: %w", err)
}
newPair, err := roomkeystore.GenerateKeyPair()
if err != nil { return nil, fmt.Errorf("generate new key: %w", err) }

predictedVersion := 1
if currentPair != nil { predictedVersion = currentPair.Version + 1 }
versioned := &roomkeystore.VersionedKeyPair{Version: predictedVersion, KeyPair: *newPair}
evt := model.RoomKeyEvent{RoomID: req.RoomID, KeyPair: versioned}
for _, s := range survivors {
    if err := h.keySender.Send(s, evt); err != nil {
        slog.WarnContext(ctx, "fan out rotated key failed", "account", s, "error", err)
    }
}

if currentPair == nil {
    if _, err := h.keyStore.Set(ctx, req.RoomID, *newPair); err != nil {
        return nil, fmt.Errorf("store new key (no prior): %w", err)
    }
} else if _, err := h.keyStore.Rotate(ctx, req.RoomID, *newPair); err != nil {
    if errors.Is(err, roomkeystore.ErrNoCurrentKey) {
        if err := h.keyStore.SetWithVersion(ctx, req.RoomID, *newPair, predictedVersion); err != nil {
            return nil, fmt.Errorf("store new key (fallback): %w", err)
        }
    } else {
        return nil, fmt.Errorf("rotate key: %w", err)
    }
}
```

- [ ] **Step 5: Add `ListRoomMemberAccounts` to `RoomStore`** if not present. Copy the query shape from `bot-notification-worker/store.go` (already `u.username` projection).

- [ ] **Step 6: Run tests, confirm PASS**. Run `make fmt && make lint SERVICE=bot-room-service && make test SERVICE=bot-room-service`.

- [ ] **Step 7: Commit**

```bash
git commit -am "feat(bot-room-service): rotate + fan out key to survivors on RemoveMember"
```

---

## Task 5: Parameterize broadcast-worker on env

**Files:**
- Modify: `broadcast-worker/main.go` — replace `stream.MessagesCanonical(cfg.SiteID)` and hardcoded consumer name with env-driven config
- Modify: `broadcast-worker/main_test.go` (or new `config_test.go`) — assert env parses correctly

- [ ] **Step 1: Read current wiring** — `grep -n "stream\.\|CreateOrUpdateConsumer\|Durable\|FilterSubject" broadcast-worker/main.go`. Identify: stream name arg, consumer Durable name, FilterSubject.

- [ ] **Step 2: Add env fields to Config**

```go
type Config struct {
    // existing…
    InputStream        string `env:"INPUT_STREAM"`         // required, no default
    InputSubjectFilter string `env:"INPUT_SUBJECT_FILTER"` // required, no default
    ConsumerName       string `env:"CONSUMER_NAME" envDefault:"broadcast-worker"`
}
```

If either required field is empty, `env` returns error → main fails fast. Match existing service pattern (grep other services for `env:"…"` without envDefault).

- [ ] **Step 3: Write failing test in `broadcast-worker/main_test.go`**

```go
func TestConfig_ParsesStreamEnv(t *testing.T) {
    t.Setenv("SITE_ID", "site-a")
    t.Setenv("INPUT_STREAM", "MESSAGES_CANONICAL_site-a")
    t.Setenv("INPUT_SUBJECT_FILTER", "chat.msg.canonical.site-a.>")
    t.Setenv("CONSUMER_NAME", "broadcast-worker")
    // set other required envs…
    var cfg Config
    require.NoError(t, env.Parse(&cfg))
    require.Equal(t, "MESSAGES_CANONICAL_site-a", cfg.InputStream)
    require.Equal(t, "chat.msg.canonical.site-a.>", cfg.InputSubjectFilter)
    require.Equal(t, "broadcast-worker", cfg.ConsumerName)
}
```

- [ ] **Step 4: Run, confirm FAIL** (fields don't exist yet).

- [ ] **Step 5: Replace hardcoded wiring in main.go**

Change any `stream.MessagesCanonical(cfg.SiteID)` → `cfg.InputStream`.
Change any hardcoded `"broadcast-worker"` durable name → `cfg.ConsumerName`.
Change FilterSubject arg → `cfg.InputSubjectFilter`.

- [ ] **Step 6: Run tests + lint** — `make fmt && make lint SERVICE=broadcast-worker && make test SERVICE=broadcast-worker`. Fix compile errors from removed imports.

- [ ] **Step 7: Commit**

```bash
git commit -am "refactor(broadcast-worker): parameterize input stream/subject/consumer on env"
```

---

## Task 6: Delete bot-broadcast-worker + add bot deploy

**Files:**
- Delete: `bot-broadcast-worker/` (entire dir)
- Move: `broadcast-worker/deploy/*` → `broadcast-worker/deploy/user/*`
- Create: `broadcast-worker/deploy/bot/Dockerfile` (identical to user or symlink), `broadcast-worker/deploy/bot/docker-compose.yml`, `broadcast-worker/deploy/bot/azure-pipelines.yml`
- Modify: root `docker-compose.yml` — bring up both deployments

- [ ] **Step 1: Move existing deploy files into user subdir**

```bash
mkdir -p broadcast-worker/deploy/user
git mv broadcast-worker/deploy/Dockerfile broadcast-worker/deploy/user/
git mv broadcast-worker/deploy/docker-compose.yml broadcast-worker/deploy/user/
git mv broadcast-worker/deploy/azure-pipelines.yml broadcast-worker/deploy/user/
```

- [ ] **Step 2: Create bot deploy variant**

`broadcast-worker/deploy/bot/Dockerfile` — copy user Dockerfile verbatim (same binary).
`broadcast-worker/deploy/bot/docker-compose.yml` — copy user compose, override env:

```yaml
services:
  bot-broadcast-worker:
    build:
      context: ../../..
      dockerfile: broadcast-worker/deploy/bot/Dockerfile
    environment:
      SITE_ID: site-a
      INPUT_STREAM: BOT_MESSAGES_CANONICAL_site-a
      INPUT_SUBJECT_FILTER: chat.bot.canonical.site-a.>
      CONSUMER_NAME: bot-broadcast-worker
      SERVICE_NAME: bot-broadcast-worker
      # …plus everything else the user compose has
```

`broadcast-worker/deploy/bot/azure-pipelines.yml` — copy user pipeline, change image tag to `bot-broadcast-worker`.

- [ ] **Step 3: Delete bot-broadcast-worker/**

```bash
git rm -rf bot-broadcast-worker/
```

- [ ] **Step 4: Update root docker-compose.yml** to include both broadcast-worker deployments (grep for `bot-broadcast-worker:` service block, point it at the new bot deploy dir).

- [ ] **Step 5: Verify build** — `make build SERVICE=broadcast-worker`. Verify no dangling imports point at `bot-broadcast-worker`.

- [ ] **Step 6: Run full tests** — `make test`. Any test file that imported bot-broadcast-worker types will fail; delete or move affected tests into `broadcast-worker/`.

- [ ] **Step 7: Commit**

```bash
git commit -am "refactor(broadcast-worker): delete bot-broadcast-worker; add bot deploy variant"
```

---

## Task 7: Parameterize notification-worker on env

**Files:**
- Modify: `notification-worker/main.go` — env-driven INPUT + OUTPUT stream/subject/consumer
- Modify: `notification-worker/main_test.go` — verify env wiring

- [ ] **Step 1: Add env fields**

```go
InputStream         string `env:"INPUT_STREAM"`
InputSubjectFilter  string `env:"INPUT_SUBJECT_FILTER"`
ConsumerName        string `env:"CONSUMER_NAME" envDefault:"notification-worker"`
OutputStream        string `env:"OUTPUT_STREAM"`
OutputSubjectPrefix string `env:"OUTPUT_SUBJECT_PREFIX"` // e.g. chat.push.notification.<site>
```

- [ ] **Step 2: Write failing config test** — mirror Task 5 Step 3.

- [ ] **Step 3: Replace hardcoded wiring** — grep for `stream.MessagesCanonical`, `stream.PushNotif`, subject builders that use `cfg.SiteID`, hardcoded durable names. Replace each with the corresponding env value. Where subject constructors are called (e.g., `subject.PushNotification(siteID, kind)`), replace with `cfg.OutputSubjectPrefix + "." + kind`.

- [ ] **Step 4: Run tests + lint** — `make fmt && make lint SERVICE=notification-worker && make test SERVICE=notification-worker`.

- [ ] **Step 5: Commit**

```bash
git commit -am "refactor(notification-worker): parameterize input+output stream/subject/consumer on env"
```

---

## Task 8: Delete bot-notification-worker + BotNotification + add bot deploy

**Files:**
- Delete: `bot-notification-worker/` (entire dir)
- Delete: `BotNotification` type from `pkg/model/bot.go` (keep other bot types)
- Move: `notification-worker/deploy/*` → `notification-worker/deploy/user/*`
- Create: `notification-worker/deploy/bot/{Dockerfile, docker-compose.yml, azure-pipelines.yml}`
- Modify: root `docker-compose.yml`

- [ ] **Step 1: Move existing deploy → user subdir** (same shape as Task 6 Step 1).

- [ ] **Step 2: Create bot deploy variant** (same shape as Task 6 Step 2) with env:

```yaml
INPUT_STREAM: BOT_MESSAGES_CANONICAL_site-a
INPUT_SUBJECT_FILTER: chat.bot.canonical.site-a.>
OUTPUT_STREAM: BOT_PUSH_NOTIF_site-a
OUTPUT_SUBJECT_PREFIX: chat.bot.notification.push.site-a
CONSUMER_NAME: bot-notification-worker
SERVICE_NAME: bot-notification-worker
```

- [ ] **Step 3: Delete `bot-notification-worker/`** — `git rm -rf bot-notification-worker/`.

- [ ] **Step 4: Delete `model.BotNotification`** — remove the type from `pkg/model/bot.go`. Grep repo for `model.BotNotification` and `BotNotification{` to ensure no remaining references outside bot-push-notification-service (fixed in Task 9).

- [ ] **Step 5: Update root docker-compose.yml** to include both notification-worker deployments.

- [ ] **Step 6: Run full build + tests** — `make build SERVICE=notification-worker && make test`. Fix any test file that imported deleted types.

- [ ] **Step 7: Commit**

```bash
git commit -am "refactor(notification-worker): delete bot-notification-worker + BotNotification; add bot deploy variant"
```

---

## Task 9: Rename bot-push-notification-service → push-notification-service; parameterize

**Files:**
- Rename: `bot-push-notification-service/` → `push-notification-service/`
- Modify: `push-notification-service/main.go` — env-driven INPUT stream/subject/consumer; consume `model.PushNotificationEvent`
- Modify: `push-notification-service/handler.go` — switch dispatcher wire type from `BotNotification` to `PushNotificationEvent`
- Modify: `push-notification-service/handler_test.go` — update fixtures
- Create: `push-notification-service/deploy/user/…`, `push-notification-service/deploy/bot/…`
- Modify: root `docker-compose.yml`

- [ ] **Step 1: Rename directory + Go module path**

```bash
git mv bot-push-notification-service push-notification-service
find push-notification-service -name '*.go' -exec sed -i 's|bot-push-notification-service|push-notification-service|g' {} +
```

- [ ] **Step 2: Switch handler wire type**

```go
// push-notification-service/handler.go
type Dispatcher interface {
    Dispatch(ctx context.Context, evt *model.PushNotificationEvent) error
}

// HandleJetStreamMsg unmarshal target:
var evt model.PushNotificationEvent
if err := json.Unmarshal(msg.Data(), &evt); err != nil { … }
if err := h.dispatcher.Dispatch(ctx, &evt); err != nil { … }
```

`LogDispatcher.Dispatch` logs `evt.ID`, `evt.RoomID`, `len(evt.Accounts)`, `evt.Data.MessageID`.

- [ ] **Step 3: Add env fields to Config**

```go
InputStream        string `env:"INPUT_STREAM"`
InputSubjectFilter string `env:"INPUT_SUBJECT_FILTER"`
ConsumerName       string `env:"CONSUMER_NAME" envDefault:"push-notification-service"`
```

Replace hardcoded stream/subject/consumer in main.go.

- [ ] **Step 4: Update handler_test.go** — swap `Notification{…}` fixtures for `PushNotificationEvent{ID: "m1-b0", RoomID: "r1", Accounts: []string{"u1"}, …}`.

- [ ] **Step 5: Create deploy/user + deploy/bot** (same shape as Task 6). Bot env:

```yaml
INPUT_STREAM: BOT_PUSH_NOTIF_site-a
INPUT_SUBJECT_FILTER: chat.bot.notification.push.site-a.>
CONSUMER_NAME: bot-push-notification-service
SERVICE_NAME: bot-push-notification-service
```

- [ ] **Step 6: Update root docker-compose.yml** to include both push-notification-service deployments.

- [ ] **Step 7: Run full build + tests + lint** — `make fmt && make lint && make test`.

- [ ] **Step 8: Commit**

```bash
git commit -am "refactor(push-notification-service): rename from bot-push-notification-service; parameterize on env; consume PushNotificationEvent"
```

---

## Task 10: Docs update

**Files:**
- Modify: `docs/superpowers/specs/2026-07-22-bot-cross-site-routing-design.md` — replace `bot-broadcast-worker`/`bot-notification-worker`/`bot-push-notification-service` references with unified `broadcast-worker[bot]`/`notification-worker[bot]`/`push-notification-service[bot]` naming
- Verify: `docs/client-api.md` — push shape is server-internal; likely no client-visible wire change, but grep for `BotNotification` references

- [ ] **Step 1: Update routing design doc** — sequence diagram + prose. Reflect unified architecture.

- [ ] **Step 2: Grep for lingering references** — `grep -rn "bot-broadcast-worker\|bot-notification-worker\|BotNotification" docs/ pkg/`. Fix each.

- [ ] **Step 3: Commit**

```bash
git commit -am "docs: update routing design + client-api references for unified pipeline"
```

---

## Task 11: Squash + polish + push

- [ ] **Step 1: Run simplify** — `/simplify` on the branch diff. Apply mechanical cleanups only.

- [ ] **Step 2: Run trim-comments** — `/remove_comments` on the branch diff. Delete WHAT-comments, shorten remaining to ≤2 lines.

- [ ] **Step 3: Full test + lint + SAST** — `make fmt && make lint && make test && make sast`. Fix anything red.

- [ ] **Step 4: Squash into logical commits** — target 2-4 commits max:
  1. `feat(bot-room-service): room key lifecycle (create/DM-ensure/add/remove)`
  2. `refactor: unify broadcast + notification + push into single-binary two-deployment (delete bot-broadcast-worker + bot-notification-worker + BotNotification; rename bot-push-notification-service→push-notification-service; parameterize on env)`
  3. `docs: unified pipeline design + routing refresh`

  Use interactive rebase or `git reset --soft origin/claude/bot-implementation-8d2ctl && git commit -m …` per group.

- [ ] **Step 5: Force-push with lease** — `git push --force-with-lease origin claude/bot-implementation-8d2ctl`.

- [ ] **Step 6: Trigger CodeRabbit re-review** — post `@coderabbitai review` on the PR.

---

## Self-review

**Spec coverage** — walking the spec:
- ✅ Delete `bot-broadcast-worker/` — Task 6
- ✅ Delete `bot-notification-worker/` — Task 8
- ✅ Rename `bot-push-notification-service/` → `push-notification-service/` — Task 9
- ✅ Parameterize each on env — Tasks 5, 7, 9
- ✅ Two deployments per service — Tasks 6, 8, 9
- ✅ Bot adopts `PushNotificationEvent`; delete `BotNotification` — Task 8, 9
- ✅ Room key on create — Task 1
- ✅ Room key on DM-ensure — Task 2
- ✅ Fan-out on add — Task 3
- ✅ Rotation on remove — Task 4
- ✅ Docs update — Task 10
- ✅ Squash + polish — Task 11

**Placeholder scan** — no TBDs, TODOs, or vague "handle edge cases" phrasing.

**Type consistency** — `roomkeystore.VersionedKeyPair`, `roomkeystore.RoomKeyPair`, `roomkeysender.Sender.Send`, `model.RoomKeyEvent`, `model.PushNotificationEvent` used consistently across tasks. `KeySender` interface introduced in Task 1 is reused throughout.

**Gotcha coverage** — bot-room-service must have subscription writer populate fields notification-worker reads (`Muted`, `HistorySharedSince`) — not currently in the plan. **Adding:**

## Task 4b: Populate Muted + HistorySharedSince on bot subscriptions

Between Task 4 and Task 5.

- [ ] Grep `bot-room-service/store_mongo.go` for `UpsertSubscription`. Ensure the doc includes `muted: false, historySharedSince: null` on insert (or omit — `notification-worker.MemberCache` should treat missing/nil as false/unset). Verify with a compat test in `notification-worker/handler_test.go` using a bot-shaped subscription doc.
- [ ] Commit: `fix(bot-room-service): ensure subscription docs are compat with notification-worker MemberCache`

---

**Plan complete and saved to `docs/superpowers/plans/2026-07-22-bot-pipeline-unification.md`.**
