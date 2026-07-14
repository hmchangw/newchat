# Reactions work out-of-the-box: built-in standard emoji + split error

**Date:** 2026-06-24
**Status:** Approved (design)
**Issue:** #382
**Scope:** Make `msg.react` accept a built-in set of well-known emoji shortcodes without per-site DB seeding, and split the collapsed validation error. Backend-only, one PR. No client/UI change required.

## Background

Adding a reaction (`...request.room.{roomID}.{siteID}.msg.react`) validates the shortcode through `pkg/emoji.Validator.Validate`. Today the only source of truth is the per-site `custom_emojis` Mongo collection — there is **no built-in / Unicode emoji set**. A fresh deployment (and every local docker-compose stack) starts with an empty `custom_emojis`, so **every** `msg.react` fails with `invalid reaction shortcode`, for every shortcode, until an admin populates the collection by hand (there is no register API).

Separately, `history-service/internal/service/reactions.go:38` maps two distinct validator errors — `ErrInvalidShortcode` (bad wire format) and `ErrUnknownShortcode` (well-formed but not registered) — to one client message, so a client cannot tell "typo" from "not a known emoji".

## Problem (verified against `main`)

- `pkg/emoji/emoji.go` `Validate`: 256-byte cap → NFC → regex `^[a-z0-9_+-]{1,32}$` (`:54`) → `CustomEmojiExists` (`:57`) → `ErrUnknownShortcode` (`:62`). No standard-set fallback.
- `history-service/internal/service/reactions.go:37-38`: `if errors.Is(err, ErrInvalidShortcode) || errors.Is(err, ErrUnknownShortcode) { return BadRequest("invalid reaction shortcode") }` — collapsed.
- No seed: docker-local seeds only Cassandra; there is no `custom_emojis` seeder anywhere.

## Design

### 1. Built-in standard-emoji allowlist (`pkg/emoji`)

Add a static set of bare, wire-valid shortcode names that `Validate` checks **before** the custom-store lookup. The `custom_emojis` store stays purely additive (per-site extras).

- **Set membership:** a generated `map[string]struct{}` of names matching `^[a-z0-9_+-]{1,32}$`, sourced from `github.com/kyokomi/emoji`'s `CodeMap()` (MIT, gemoji-derived, GitHub-convention shortcodes), colons stripped, regex-filtered (drops skin-tone/`.`/`'` variants so the set stays consistent with the format gate). Only the names are needed — reactions store the shortcode string; the client renders the glyph.
- **No new runtime dependency in the main module.** The set ships as a **committed generated file** (`pkg/emoji/standard_emoji_gen.go`). The generator that imports `kyokomi/emoji` lives in an **isolated nested module** (`pkg/emoji/gen/` with its own `go.mod`), invoked by a `//go:generate` directive, so `kyokomi/emoji` never enters the main `go.mod`. This honours the repo's "ask before adding a third-party dependency" rule while keeping the set reviewable in-diff and regenerable.
- **Validator order:** 256-cap → NFC → regex → **standard-set lookup (present → return NFC shortcode, nil — before any Mongo call)** → existing custom-store lookup → `ErrUnknownShortcode`. The standard check is an allocation-free map lookup on the hot path, before the custom lookup.

### 2. Split the collapsed error (`history-service/internal/service/reactions.go`)

- `emoji.ErrInvalidShortcode` → `BadRequest("invalid reaction shortcode")` (bad format).
- `emoji.ErrUnknownShortcode` → `BadRequest("unknown reaction shortcode")` (well-formed, not registered).

### 3. Docs ratchet

Update the `docs/client-api.md` reaction section: document the built-in standard set + the two distinct error reasons.

## Path trace (7 segments)

1. client request / `docs/client-api.md` request side — **N/A** (`msg.react` request unchanged).
2. gatekeeper mapping — **N/A**.
3. canonical model / event structs — **N/A** (the `ReactionDelta` shortcode field is unchanged).
4. workers — **N/A** (validation lives in history-service's reaction write path).
5. storage — **N/A** (no schema change; `custom_emojis` stays additive).
6. **read/history — change:** `pkg/emoji.Validate` gains the standard-set check; `reactions.go:38` splits the error.
7. **client-facing events — doc change:** `docs/client-api.md` reaction section (standard set + split error reasons).

## Acceptance criteria

- A fresh stack with an empty `custom_emojis` accepts a standard shortcode (`thumbsup`, `+1`, `heart`) with no seeding.
- A site-registered custom shortcode still validates (custom path unchanged; cache TTL behaviour unchanged).
- Unregistered, non-standard, well-formed shortcode → `unknown reaction shortcode`; malformed → `invalid reaction shortcode`.
- Standard-set membership is an allocation-free map lookup, before the Mongo/custom lookup.
- Main `go.mod` gains no new dependency.
- `docs/client-api.md` reaction section updated in the same PR.

## Out of scope

- A list/register custom-emoji RPC (separate feature; none today).
- Register-time cache invalidation for the 60s lookup cache (operator note only).
