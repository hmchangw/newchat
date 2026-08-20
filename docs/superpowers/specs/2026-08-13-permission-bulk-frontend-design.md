# Permission Bulk Update & Resend (admin-frontend round) — Design

**Status:** Approved, ready for planning
**Date:** 2026-08-13 (resend mechanism revised same day: re-POST → minimal resync);
amended 2026-08-17 (post-merge review round: chunk 5,000 → 1,000 accounts/event for the
128KB broker `max_payload`; per-destination fanout lanes; resync hands every state group
to one fanout call; fanout budget `FANOUT_TIMEOUT` 30s, validated at startup below the 40s
HTTP write timeout)
**Builds on:** `2026-08-10-user-permission-whitelist-design.md` (as amended 2026-08-12).
This is the admin-frontend round that spec deferred, plus the request-limit removals and
fanout chunking that the bulk requirement forced onto the backend.

*As-shipped note: this spec describes deltas against a pre-rewrite branch state. The shipped history was rebuilt, so the removals it prescribes (`bodyLimit`, `permission.get`, `MaxSubjects`/`EvaluateGrant`/`SiteID`) never appear in the final commits, and some prescribed shapes (`stateGroupKey`, `GetUserSettingsAndPermissions`) were superseded by later cleanup. The code is the source of truth.*

## 1. Overview

Two requirements drive this round:

1. An admin must be able to update permissions for very large batches — the stated need
   is 1,000 users at once. The design removes the artificial caps entirely rather than
   raising them: no `MaxSubjects`, no request-body limit.
2. When fanout to a site fails, the admin must be able to **re-send from the console**
   to restore cross-site consistency — implemented as an idempotent **resync endpoint**
   that re-delivers recorded state without re-recording anything.

Everything lands in admin-service (producer side) and admin-frontend. inbox-worker,
user-service, and the `pkg/model` event/state shapes are untouched; the wire-contract
changes are the deleted `MaxSubjects` constant and one new admin endpoint (§5).

## 2. Decisions

| Decision | Choice | Rejected alternatives |
|---|---|---|
| Resend mechanism | **Minimal resync endpoint** — `POST /v1/admin/permissions/resync` re-fans-out the current materialized state from the local `users` docs; writes no ledger, audit, or user-doc rows | OUTBOX auto-retry (recommended, declined); re-POST of the identical body (chosen first on 2026-08-13, revised the same day once its duplicate-audit cost was weighed); persisted failure list + console list page (still declined) |
| Bulk input | **Paste-list mode** toggle beside the existing AccountPicker | CSV upload; both |
| Subject count cap | **Removed** (`MaxSubjects` deleted) | keep 200; raise to 1,000 or 10,000 |
| Request body cap | **Removed** (1MB middleware deleted) — user decision 2026-08-13, **against the recorded recommendation**; risk §9.1 | keep 1MB |
| NATS payload ceiling | **Chunked fanout**, 1,000 accounts per event (5,000 until 2026-08-17 — the brokers run a 128KB `max_payload`, not NATS's 1MB default) | single event (would cap a batch at ~20k accounts) |
| Mongo transaction | **Single transaction kept** — all-or-nothing preserved at any N | chunked transaction (loses atomicity) |

Consequence the resend choice accepts (unchanged by the revision): the failure
information lives only in the open dialog — closing it forfeits the console resend entry
point (server logs remain the only trace). The resync itself writes nothing and is
idempotent, so repeated retries cost nothing.

## 3. Limit removals (backend)

### 3.1 `MaxSubjects` — deleted

- `pkg/model/permission.go`: the `MaxSubjects` constant is removed (`MaxReasonRunes`
  stays).
- Validation: "non-empty and ≤ `MaxSubjects`" becomes **non-empty only**;
  `PermissionInvalidSubjects` (`invalid_subject_count`) is kept for the empty case.
- The frontend cap-mirror constant is deleted with it.

### 3.2 Request body limit — deleted

- `admin-service/middleware.go`: `maxRequestBodyBytes` and `bodyLimit()` removed;
  `main.go` drops the `r.Use(bodyLimit(...))` line; the middleware tests go with them.
  This reverts the hardening this same branch introduced.
- Recorded as an accepted risk (§9.1): JSON parsing precedes validation, so **no
  app-level bound on request memory remains**. Surviving mitigations: the surface is
  authenticated (`requireAdmin` rejects on headers before the body is read), server
  read timeouts, and whatever ops-owned infrastructure limits exist (not verifiable
  from this repository — no gateway or ingress config lives here).

### 3.3 What still bounds a batch

No fixed number anywhere. In practice: request memory (unbounded by decision), then the
single Mongo transaction — 2N documents modified per batch; tens of thousands is the
practical comfort zone, and an oversized batch fails **cleanly** (500, atomic, nothing
partial, retryable). NATS is out of the equation thanks to chunking (§4). The only guard
against an accidental whole-company paste is the visible count in the console (§6.2).

## 4. Chunked fanout (admin-service)

```go
// ≈90KB per INBOX envelope at 64 bytes/account (the envelope base64-encodes the payload)
// under the 128KB max_payload our brokers run — retuned from 5000 on 2026-08-17.
const fanoutChunkSize = 1000
```

- The publish loop becomes: for each remote site × each chunk of ≤1,000 accounts → one
  `UserPermissionsUpdated` event. Same event shape; `Accounts` carries the chunk;
  `Permission`, `State`, and `Timestamp` are identical across all chunks of one batch.
- 2026-08-17: each destination walks every chunk in its **own goroutine lane** under one
  shared budget (`FANOUT_TIMEOUT`, default 30s, validated at startup to stay below the
  40s HTTP write timeout so `syncFailures` is always deliverable). A stalled peer burns
  only its own lane; once the budget is gone the lane stops and is reported once.
- A failed publish marks the destination in `syncFailures` (listed once) and the lane
  **continues** through its remaining chunks — maximize delivery, aggregate failures per
  destination. The response shape is unchanged.
- Partial chunk delivery is safe by construction: remote applies are per-account guarded
  and idempotent, so chunks may arrive in any order, duplicated, or not at all; a resync
  re-publishes every chunk and already-applied accounts no-op.
- **inbox-worker: zero changes.** It sees the same event type with shorter account
  arrays, possibly several per batch.
- The chunked-fanout function is **shared** by the write path (§ main spec 4.1 step ④)
  and the resync endpoint (§5).
- The OUTBOX upgrade path recorded in the main spec §5.3 is unaffected — the chunk loop
  would simply publish `OutboxEvent`s instead of direct INBOX events.

## 5. Resync endpoint (admin-service)

`POST /v1/admin/permissions/resync`, registered under `requireAdmin` beside the existing
permission routes.

**Request:** `{"permission": "external.image.view", "accounts": ["alice", "bob", …]}`

**Semantics: re-delivery, not re-recording.** The endpoint reads each account's current
materialized state from the local `users` collection (one batch read, projection
`{account, permissions.<field>}`), groups the accounts by identical state — one group per
original batch in the common case; more if another write landed in between — and hands
every group to **one** chunked fanout call (§4; 2026-08-17: it used to call the fanout
once per group, which let a peer stalled on the first group starve healthy peers of the
later ones). It always delivers the **current** truth, never
the original request's possibly-stale state. It writes nothing: no ledger rows, no audit
entries, no user-doc updates.

- **Validation:** known permission key (`unknown_permission`); `accounts` non-empty
  (`invalid_subject_count`). **No existence or `IsActive` checks** — delivering
  already-recorded state grants nothing new. Accounts with no local user doc or no state
  for the key are silently skipped (they have nothing to sync).
- **Response:** `{"syncFailures": […]}` — same field semantics as the write path
  (destinations whose INBOX publish was not acknowledged); empty means every publish was
  acknowledged. Status 200.
- **Idempotent by construction:** the fanned-out states carry their stored `updatedAt`
  watermarks, so remote guards no-op anything already applied. Safe to call repeatedly.
- **No audit entry** — deliberate: the operation mutates nothing; server logs carry the
  operational trace.
- Store addition: a batch read, e.g. `GetUserPermissionsForAccounts(ctx, accounts)
  (map[string]*model.UserPermissions, error)` (exact shape finalized in the plan);
  the existing single-account `GetUserPermissions` stays for `currentlyGranted`.

## 6. Console (admin-frontend)

Exact user-facing copy follows the console's existing language and tone; strings below
describe intent.

### 6.1 Paste-list input mode

`CreatePermissionsDialog` gains a two-mode subject input — **pick mode** (the existing
AccountPicker, unchanged) and **paste mode** (a textarea):

- Parse on `/[\s,;]+/` (whitespace, newlines, commas, semicolons), trim, drop empties.
  Accounts are sent verbatim — no case normalization, matching backend semantics.
- Live feedback: deduped count plus how many duplicates were dropped ("N accounts,
  M duplicates removed"). Zero parsed accounts disables submit.
- Modes never merge: **the active mode's list is what submits.** The toggle shows a
  per-mode count badge (e.g. `Pick (5) / Paste (995)`) so "picked 5, pasted 995,
  thought I sent 1000" cannot happen silently.

### 6.2 Count-visible submit

The submit control always carries the effective (deduped, active-mode) count — "Grant to
25,431 users" / "Revoke from 12 users". With the caps gone, this visibility is the only
guard against pasting the wrong file; it is a requirement, not a nicety.

### 6.3 Result view — `syncFailures` banner + resync

- When the 201 response carries `syncFailures`, the result view shows a warning banner —
  "recorded and effective at this site, but sync to site-b failed" — with a **resend**
  button.
- The button calls **`POST /v1/admin/permissions/resync`** with `{permission, accounts}`
  from the just-submitted request (both are still in the dialog's state). It locks while
  in flight; on completion the banner reflects the resync response (empty
  `syncFailures` → "sync complete"). Because resync writes nothing, the button may be
  pressed repeatedly without cost.
- The button renders only while the latest response's `syncFailures` is non-empty, and
  it lives and dies with the dialog (accepted with the mechanism choice, §2).

### 6.4 Offender-list scaling + one-click strip

`unknown_accounts` / `inactive_subject` rejections can now name hundreds of accounts:

- Scrollable offender list plus a copy-list control.
- A **"remove these accounts"** action strips the offenders from the active input
  (paste text or picker selection) so the admin can resubmit immediately. Backend
  all-or-nothing semantics are unchanged — the convenience lives entirely client-side.

### 6.5 Long result lists

`duplicatesIgnored` (and `grants` at large N) render as counts with a collapsed,
expandable list instead of dumping thousands of rows.

## 7. Testing

| Area | Cases |
|---|---|
| admin-service validation | empty list still rejected (`invalid_subject_count`); a large list (e.g. 10,001 accounts) accepted; body-limit middleware tests removed |
| chunked fanout (unit, captured publish func) | N ≤ 1,000 → one event per dest; 1,001 → two; a worst-case chunk envelope stays under 128KB; a stalled peer consumes only its own lane; chunk boundaries exact and complete; a dest failing on any chunk → listed once in `syncFailures` while other chunks/dests are still attempted; per-chunk payload shape (same state/timestamp, chunked accounts) |
| resync handler (unit) | groups accounts by identical state and fans out all groups in one call (one shared deadline); accounts with no doc/state are skipped; **no ledger/audit/user-doc writes** (store mock asserts no mutating calls); unknown key → `unknown_permission`; empty accounts → `invalid_subject_count`; `syncFailures` aggregation matches the write path |
| admin-frontend (Vitest, existing dialog patterns) | paste parsing (separators, trim, dedup, count); zero-parse disables submit; mode badges + active-mode-wins payload; submit-label count; `syncFailures` banner + button posts `{permission, accounts}` to resync (not the original body) + in-flight lock + banner update from the resync response; strip action filters both input modes; long-list collapse |

## 8. Documentation

- `docs/client-api.md` §9.13: `subjectAccounts` constraint → "non-empty (no fixed
  cap)"; `invalid_subject_count` description → the empty-list case; drop the
  oversized-body 400 row.
- `docs/client-api.md` new **§9.15 Resync permission fanout**: request/response field
  tables, semantics (re-delivery only, writes nothing, idempotent), error table, curl
  example.
- `docs/client-api/request-reply.md`: mirror both in the same PR.
- Main spec touch-ups in the same change (kept consistent rather than drifting):
  `MaxSubjects` removed from §3.1, §4.3 step wording, §5.1 chunking note, §5.4
  remediation now resync-first, §14 body-limit bullet flipped to a removal note,
  §1.2/§7 deferral notes now point here, §16/§17 cross-references.

## 9. Residual risks

1. **Unbounded request body** (decision 2026-08-13, against the recorded
   recommendation). No app-level memory bound on admin-service requests; any holder of
   a valid admin session (leaked token, buggy internal script) can submit an
   arbitrarily large body that is fully parsed before any validation runs. Remaining
   mitigations: authentication precedes the body read; server read timeouts; ops-layer
   limits if any. Rollback is re-adding the same 15 lines this round deletes.
2. **Huge-N transaction behavior.** Never partial (single transaction); an absurd batch
   fails clean (500) or runs slow. Practical comfort zone: tens of thousands of
   subjects.
3. **Resend caveats.** Closing the dialog forfeits the console resend entry point; the
   fallback for a lost dialog (or the commit→fanout crash window) is re-submitting the
   form, which — unlike resync — does insert a fresh batch of ledger/audit rows
   (harmless, rare). Resync leaves no audit trace by design; server logs carry it.
4. **Accidental mass grant.** With the caps gone, the visible count (§6.2) is the only
   guard against pasting the wrong file into a security permission.
5. **Partial chunk delivery window.** Until a resync succeeds, a failed destination may
   hold some chunks of a batch and not others — per-account staleness that converges on
   resync or on the next change to the affected accounts.
