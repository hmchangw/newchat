# Broadcast-Worker Encryption Toggle — Design

**Date:** 2026-05-03
**Service:** `broadcast-worker`
**Branch:** `claude/broadcast-encryption-toggle-ZGc4m`

## Summary

Add an environment-variable toggle to enable or disable message-content encryption in `broadcast-worker`. The default is **off**. When off, channel `RoomEvent` payloads carry plaintext `Message` instead of `EncryptedMessage`, and the worker does not connect to Valkey at startup. When on, behavior is unchanged from today.

## Motivation

Encryption is currently mandatory: `broadcast-worker` requires `VALKEY_ADDR` at startup, fetches a per-room key on every channel publish, and NAKs the message if no key exists. This is friction for local development, fresh sites, and deployments that don't yet have key-rotation infrastructure in place. A toggle lets operators run the worker without Valkey while the encryption pipeline is rolled out site-by-site.

## Configuration

A new grouped config struct on `broadcast-worker/main.go`:

```go
type encryptionConfig struct {
    Enabled bool `env:"ENABLED" envDefault:"false"`
}

type config struct {
    // ... existing fields ...
    ValkeyAddr           string           `env:"VALKEY_ADDR"`
    ValkeyPassword       string           `env:"VALKEY_PASSWORD"           envDefault:""`
    ValkeyKeyGracePeriod time.Duration    `env:"VALKEY_KEY_GRACE_PERIOD"`
    Bootstrap            bootstrapConfig  `envPrefix:"BOOTSTRAP_"`
    Encryption           encryptionConfig `envPrefix:"ENCRYPTION_"`
}
```

Changes to existing config:

- `VALKEY_ADDR` loses its `,required` tag.
- `VALKEY_KEY_GRACE_PERIOD` loses its `,required` tag.
- `VALKEY_PASSWORD` is unchanged.

Validation lives in `main()`, combined with Valkey wiring (single flag check):

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

The shutdown hook for `keyStore.Close()` is registered only when `keyStore != nil`.

Startup log gains an `encryption` field:

```go
slog.Info("broadcast-worker started", "site", cfg.SiteID, "encryption", cfg.Encryption.Enabled)
```

## Handler

`Handler` (in `broadcast-worker/handler.go`) gains an `encrypt bool` field and `NewHandler` gains a corresponding parameter. `publishChannelEvent` branches on the flag:

```go
if h.encrypt {
    // existing block: keyStore.Get → roomcrypto.Encode → set EncryptedMessage, nil out Message
} else {
    evt.Message = clientMsg
}
```

The DM path (`publishDMEvents`) is unchanged — it has always published plaintext.

When `encrypt=false`, `keyStore` is `nil` and is never dereferenced. The interface field stays typed as `RoomKeyProvider`; we simply do not call into it.

## Local Development

`broadcast-worker/deploy/docker-compose.yml` sets `ENCRYPTION_ENABLED=false` explicitly so the dev default mirrors production. Developers who want to exercise the encrypted path opt in by overriding the variable. The Valkey service in compose stays defined; it's just unused by the worker unless the toggle is flipped.

## Tests

Following the project's TDD rule (Red → Green → Refactor):

### Existing tests — mechanical update

Every `NewHandler(store, us, pub, keyStore)` call in `handler_test.go` and `integration_test.go` gains a trailing `true` argument so current encrypted-path coverage is preserved.

### New unit tests in `handler_test.go`

A new test `TestHandler_HandleMessage_ChannelEncryptionDisabled` covers the encryption-off channel path:

| Case | Setup | Assertion |
|------|-------|-----------|
| plaintext channel publish | `NewHandler(store, us, pub, nil, false)` | event has `Message != nil`, `EncryptedMessage == nil`, published to `subject.RoomEvent(roomID)` |
| keystore not consulted | `keyStore == nil`, mention-all message | no panic, no key lookup attempted, event still published correctly |
| mentions still resolved | message contains `@user1` | `evt.Mentions` populated, `SetSubscriptionMentions` still called on the store |

DM-path tests are unchanged.

### New integration test in `integration_test.go`

`TestHandler_ChannelPlaintext_Integration` constructs the handler with `encrypt=false`, publishes a canonical message to a channel room, and asserts the subscriber receives a `RoomEvent` with `Message` set and `EncryptedMessage` empty. Reuses the existing testcontainers fixtures (Mongo, NATS).

### Coverage

Per CLAUDE.md, target ≥80% across the package and ≥90% on the handler. The new branch is small and the new tests cover it directly.

## Consumer Impact

`RoomEvent` already carries both `Message` and `EncryptedMessage` as optional fields. Today:

- Channel `new_message` events from `broadcast-worker` set `EncryptedMessage`.
- DM `new_message` events from `broadcast-worker` set `Message`.
- `history-service` edit/delete events set `Message`.

With the toggle off, channel `new_message` events shift to the plaintext `Message` shape that DM and history events already use. Any subscriber that handles those will handle plaintext channel `new_message` events too.

**The chat frontend lives in a separate repo. Flipping the toggle on a deployment whose clients expect `EncryptedMessage` will break those clients.** This PR only adds the worker-side capability; coordinating consumers is out of scope.

## Out of Scope

- DM encryption (remains plaintext).
- Per-room toggle.
- Frontend changes.
- Metrics or tracing differentiating encrypted vs. plaintext publishes.
- Renaming or relocating the existing `VALKEY_*` env vars.

## Files Touched

- `broadcast-worker/main.go` — config struct, conditional Valkey/keystore wiring, conditional shutdown hook, startup log.
- `broadcast-worker/handler.go` — `encrypt` field on `Handler`, branching in `publishChannelEvent`.
- `broadcast-worker/handler_test.go` — update existing call sites; add `TestHandler_HandleMessage_ChannelEncryptionDisabled`.
- `broadcast-worker/integration_test.go` — update existing call sites; add `TestHandler_ChannelPlaintext_Integration`.
- `broadcast-worker/deploy/docker-compose.yml` — explicit `ENCRYPTION_ENABLED=false`.
