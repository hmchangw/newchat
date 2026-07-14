# user-service API Doc Refinement (`docs/client-api.md` §3.4) — Design

**Status:** Design approved — implementation pending.
**Date:** 2026-06-09
**Scope:** Refine the existing §3.4 user-service section of `docs/client-api.md` so it fully satisfies the API-doc guidelines, with every field verified against the Go structs. No new file; §3.4 is the single source of truth. Other sections (§3.1–3.3, §4–§6) are untouched.

## Context

The MR introduces `user-service` with 9 client-facing NATS request/reply endpoints, already documented in `docs/client-api.md` §3.4 (lines ~3011–3510). This refinement brings §3.4 to a uniform, struct-verified standard.

The 9 endpoints (the complete client-facing surface — a reviewer confirms via a full-diff sweep):
`status.getByName`, `status.set`, `subscription.list`, `subscription.getChannels`, `subscription.getDM`, `subscription.getByRoomID`, `subscription.count`, `subscription.setAppSubscription`, `apps.list`.

## Guidelines (the bar §3.4 must meet)

1. Every request body and response schema is a field **table** (Field | Type | Required/Notes).
2. Every **success** response includes a JSON example.
3. **Client-facing events only** — server outbox/inbox federation is omitted.
4. **No bare `object` type** — every field has an explicit type. Compound types are either a named §3.0 schema referenced by linked name (the house convention — counts as explicit) or an inline field table for one-offs. Literal `object` and undocumented inline `{…}` are eliminated.
5. Minimal prose — no redundant comments or long explanations; match the §3.1–3.3 house style.

## Changes

### #1 — Remove federation events
Delete `status.set`'s "Triggered events — success path" block (the cross-site `UserStatusUpdated` outbox event) and trim the intro sentence referencing it. Add one terse line in the §3.4 intro: these endpoints emit no client-facing events (the status write's only side effect is server-side federation, omitted here).

### #2 — Eliminate bare `object` / inline `{…}` types
Audit all 9 endpoints. Every `| object |` or inline `{ "k": type, … }` field becomes either:
- a linked reference to a named §3.0 shared schema (e.g. `u` → `SubscriptionUser`), or
- an inline field sub-table for a genuine one-off.
Ensure each referenced §3.0 schema (`Subscription`, `SubscriptionUser`, `ChannelRef`, `AppAssistant`, `AsyncJobResult`) exists and is itself a full field table; add/repair if missing.

### #3 — Struct verification (the correctness gate)
Every documented field — name (json tag), type, optionality (`omitempty`) — is cross-checked against the Go source of truth:
- status: `user-service/models/status.go`, `user-service/service/status.go`
- subscriptions: `user-service/models/subscription.go`, `pkg/model/subscription.go`, `user-service/service/subscriptions.go`, `user-service/mongorepo/subscriptions.go`
- apps: `user-service/models/app.go`, `pkg/model` (`AppAssistant`), `user-service/service/apps.go`
Mismatches (wrong type, missing/extra field, wrong optionality, wrong subject casing) are corrected to match the structs.

### #4 — JSON examples
Confirm all 9 success responses carry a valid JSON example consistent with their (corrected) field table; add any missing.

## Testing / Verification
No code; verification is documentary:
- Reviewer A (guideline compliance): tables present, JSON examples present, zero bare `object`, events omitted, prose minimal.
- Reviewer B (schema accuracy): field-by-field vs Go structs.
- Reviewer C (scope sweep): `git diff origin/main...HEAD` confirms the 9 endpoints are the complete new client-facing set.
- `make lint` is irrelevant (docs only); markdown must render (tables fenced correctly).

## Out of Scope
- Other service sections (§3.1–3.3), §4 Message Send, §5 Encryption, §6 error catalog.
- Any handler/code change. This is documentation-only; the handlers already exist and match.
