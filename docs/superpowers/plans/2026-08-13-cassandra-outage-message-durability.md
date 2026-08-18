# Cassandra-Outage Message Durability Implementation Plan — SUPERSEDED

**Status:** Executed, then partly superseded. Do not implement from this document.

**The shipped design is `docs/superpowers/specs/2026-08-13-cassandra-outage-message-durability-design.md`.**
Read that instead. This file is a stub; the original 2,480-line plan is in git history at
commit `a9b1056` and its ancestors.

## Why it was reduced

The plan's central mechanism — a `MESSAGES-PARKED` stream that messages were published to
when the worker gave up, with the give-up decision gated on a site-health marker — **was
removed before merge**. Leaving 2,480 lines of step-by-step instructions describing
streams, subjects, env vars and files that no longer exist is worse than leaving nothing:
the next reader would believe `MESSAGES-PARKED`, `park.go`, and `HEALTHY_PARK_AFTER` are
part of the system.

## What shipped as planned

Tasks 1, 2, 3, 4, 6 and 7 landed essentially as written, and the code is now the
authority on all of them:

| Task | Deliverable | Where it lives now |
|---|---|---|
| 1 | Consumer lag + durability metrics | `message-worker/metrics.go` |
| 2 | Per-site history-degraded marker store | `pkg/histdegrade/` |
| 3 | Marker set/clear tracking in the worker | `message-worker/degrade.go` |
| 4 | Additive `incompleteSince` on history reads | `history-service/internal/{models,service}/`, `docs/client-api.md` |
| 6 | Quote preservation across the recovery drain | `message-worker/handler.go` (`reprojectUnverifiedQuote`) |
| 7 | Thread-badge suppression during replay | `message-worker/handler.go` (`publishThreadReplyEventIfLive`) |

## What was superseded

Task 5 — the parking lane and the marker-selected retry policy — was replaced by **CQL
error classification** (design §3.3–§3.4): infra-class Cassandra errors NAK forever;
request-class errors NAK for a bounded window and are then dropped, rate-capped, behind a
kill switch.

Two problems drove the change. The health-gated policy could not work: a failing message's
own contribution to the marker destroyed the evidence needed to judge that message, which
made the park path unreachable and forced a revert. And the parking lane itself had no
consumer and no alert, so "give up safely" in practice meant "accumulate silently."

The design doc records both, including the non-goal it had to retract — an earlier revision
ruled out error classification entirely, reasoning that was sound while the alternative to
retrying was destruction and obsolete once it wasn't.

## Still accurate here

Nothing that isn't also in the design doc. The Global Constraints this plan carried are
the repo's own rules in `CLAUDE.md`; the ops prerequisites are design §3.1; the testing
requirements and the never-executed-integration caveat are design §5 and §5.1.
