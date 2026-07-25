# teams-chat-sync: fitting a 7-day backfill under a 1-day Job deadline

**Date:** 2026-07-25
**Status:** Approved

## Problem

The `teams-chat-sync` CronJob's initial backfill (from `SYNC_DEFAULT_FROM`,
2026-04-01) is projected to take ~7 days of wall-clock, but the ArgoCD policy
caps a Job's `activeDeadlineSeconds` at 1 day. Steady-state incremental sweeps
finish well under 1 day; only the one-time backfill exceeds the cap.

## Options considered

1. **Partition users into 7 buckets, one pod each.** Rejected: permanent
   complexity (bucket assignment, rebalancing, 7 manifests) for a one-time
   problem, and concurrent buckets share one Graph app+tenant throttling
   budget, so wall-clock compresses far less than 7×. The siteID vote also
   needs the full `teams_user` cache in every pod regardless of bucket.
2. **Internal cron loop inside the service.** Rejected: converts a
   run-to-completion Job into a long-running Deployment, losing CronJob-native
   success/failure history and retry semantics, and re-implements what
   `concurrencyPolicy: Forbid` already guarantees. The code's contract is that
   the run deadline is owned by the Kubernetes CronJob (`main.go`).
3. **Rely on the existing per-user watermark resume (chosen).** Each user's
   `from` watermark advances only after that user's chats fully persist, and a
   run skips users whose watermark already reached today's window. A run
   killed at the deadline loses nothing; successive scheduled runs drain the
   backlog (~7 calendar days), after which incremental sweeps fit easily.

## Decision

Keep the CronJob run-to-completion model. Operate it as:

- `schedule`: daily (or a few times per day during backfill).
- `activeDeadlineSeconds: 82800` (23 h — inside the ArgoCD 1-day cap, with
  margin for graceful SIGTERM handling).
- `concurrencyPolicy: Forbid` + `startingDeadlineSeconds` — Kubernetes-native
  "next run starts only after the current completes".

These are ops/ArgoCD manifest settings; no Kubernetes manifests live in this
repo.

Code change in this repo (this spec's implementation):

1. **`ListUsers` returns users ordered by watermark ascending** — documents
   without `from` (never synced) first, then oldest `from` first — via a Mongo
   server-side sort (`from: 1`; BSON orders missing before Date). The syncer
   dispatches in slice order, so each deadline-bounded run spends its budget
   on the stalest users and progress through the backlog is monotonic, no
   matter how Mongo's natural order interleaves finished and unfinished users.
2. **Index `{from: 1}` on `teams_user`**, created in this service's
   `EnsureIndexes`. teams-chat-sync owns the `from` field (teams-user-sync
   creates no indexes on the collection), so it owns this index. The sort is
   then index-served rather than an in-memory sort over the full collection.

### Semantics note

The Mongo sort orders by the raw `from` field, with missing-`from` users
first. This equals "effective watermark ascending" as long as
`SYNC_DEFAULT_FROM` is not later than any persisted watermark — true in
practice, since watermarks are only ever written as `startOfDay(now)` at run
time. If an operator raises `SYNC_DEFAULT_FROM` above existing watermarks,
never-synced users are still dispatched first, which is a harmless (and
arguably desirable) priority inversion.

## Testing

- Integration (testcontainers, `//go:build integration`):
  - `ListUsers` returns never-synced users first, then ascending `from`.
  - `EnsureIndexes` creates the `teams_user` `{from: 1}` index and stays
    idempotent.
- No syncer changes: dispatch already preserves `ListUsers` order.
