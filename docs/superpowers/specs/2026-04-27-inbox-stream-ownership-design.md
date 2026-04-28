# Inbox Stream Ownership

## Problem

`INBOX_{siteID}` is currently created by **two** services in dev mode (`BOOTSTRAP_STREAMS=true`):

- `inbox-worker/bootstrap.go` creates it with `Name` only (no `Subjects`).
- `search-sync-worker` creates it with `Name + Subjects + Sources + SubjectTransforms` via `inboxBootstrapStreamConfig` (`search-sync-worker/inbox_stream.go`).

Whichever service runs second overwrites the first via `CreateOrUpdateStream`, producing a race. If `inbox-worker` wins, federation breaks (no Sources). If `search-sync-worker` wins, federation works but only because of code that was always documented as a temporary scaffold — the file's own comment reads:

> "This helper stays local to search-sync-worker because it's bootstrap-only — inbox-worker will own an equivalent construction (as a proper feature, not a test toggle) when it migrates in its own PR."

That migration never happened. Until now.

## Goals

- Single owner for INBOX bootstrap.
- App code owns the schema (`Name + Subjects`); ops/IaC owns the federation topology (`Sources + SubjectTransforms`). No app code touches federation config.
- Local single-site dev works without manual NATS configuration.
- Zero regressions in `search-sync-worker` integration tests.

## Non-Goals

- Multi-site federation in local dev. If a developer ever needs it, they configure `Sources` manually via NATS CLI or a dedicated test fixture; not in app `bootstrap.go`.
- Changing `pkg/stream/stream.go` `Inbox()` subject patterns. The current two-pattern split (`chat.inbox.{site}.*` + `chat.inbox.{site}.aggregate.>`) stays — it provides broker-level typo defense for known event shapes.
- Changing the `inbox-worker` runtime behavior (consumer config, handler logic).

## Design

### Ownership table

| Concern | Owner |
|---|---|
| Stream existence (`Name`) | App code in dev (`inbox-worker/bootstrap.go`) / ops/IaC in prod |
| Stream schema (`Subjects`) | **App code** in both dev and prod-equivalent terms — the canonical source is `pkg/stream.Inbox(siteID)` |
| Federation (`Sources` + `SubjectTransforms`) | **Ops/IaC only** — never app code, never test toggles |
| Consumer creation | App code, always |

### Production reference (set by ops/IaC, illustrative)

For our home site `site-us` federating with `site-eu` and `site-apac`:

```yaml
Name: INBOX_site-us
Subjects:
  - chat.inbox.site-us.*
  - chat.inbox.site-us.aggregate.>
Sources:
  - Name: OUTBOX_site-eu
    SubjectTransforms:
      - Source: outbox.site-eu.to.site-us.>
        Destination: chat.inbox.site-us.aggregate.>
  - Name: OUTBOX_site-apac
    SubjectTransforms:
      - Source: outbox.site-apac.to.site-us.>
        Destination: chat.inbox.site-us.aggregate.>
Storage / Replicas / Retention / MaxAge / MaxBytes: per ops policy
```

`inbox-worker` consumes the entire stream (no `FilterSubject`) and gets a unified flow of local + federated events. The subject reveals origin (`.aggregate.` segment present or not).

### Code changes

**`inbox-worker/bootstrap.go`** — change the helper body from passing `Name` only to passing `Name + Subjects`:

```go
inboxCfg := stream.Inbox(siteID)
if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
    Name:     inboxCfg.Name,
    Subjects: inboxCfg.Subjects,
}); err != nil {
    return fmt.Errorf("create INBOX stream: %w", err)
}
```

Update the surrounding doc comment to say the helper creates the schema and that federation is owned by ops/IaC.

**`inbox-worker/bootstrap_test.go`** — replace the "Subjects must be empty" assertion with "Subjects equals `pkg/stream.Inbox(siteID).Subjects`". Keep the disabled / enabled / error cases.

**`search-sync-worker/inbox_stream.go`** — delete `inboxBootstrapStreamConfig` and update the `inboxMemberCollection` doc comment (lines 60-66) to point at `inbox-worker` as the INBOX owner instead of "see inboxBootstrapStreamConfig".

**`search-sync-worker/inbox_stream_test.go`** — delete the entire file (it only tests the deleted function).

**`search-sync-worker/main.go`** —
1. Remove the `RemoteSiteIDs []string` field from `bootstrapConfig` and update the surrounding comments.
2. In the bootstrap loop, where it currently does `if streamCfg.Name == inboxName { bootstrapCfg = inboxBootstrapStreamConfig(...) }`, change to `if streamCfg.Name == inboxName { continue }` — search-sync-worker no longer creates INBOX. Add a comment explaining inbox-worker owns it.
3. The `inboxName := stream.Inbox(cfg.SiteID).Name` local stays since the loop still needs it to identify the INBOX entry to skip.

**`search-sync-worker/deploy/docker-compose.yml`** — no change needed. The compose already has `BOOTSTRAP_STREAMS=true` (we added it earlier in this PR). The flag still applies to other collections' streams (if any). `BOOTSTRAP_REMOTE_SITE_IDS` is not currently set in compose, so removing the field is invisible.

**`CLAUDE.md`** — extend the "Stream bootstrap is opt-in" bullet under "JetStream Streams" with the ownership table from the spec, so future services know that `Sources` / `SubjectTransforms` belong to ops/IaC, not app code.

### Test strategy

Per CLAUDE.md, every change follows Red-Green-Refactor.

1. **`inbox-worker/bootstrap_test.go`** — flip the assertion. Run; fail (current test expects empty Subjects). Update helper. Run; pass.
2. **`search-sync-worker/inbox_stream_test.go` deletion** — runs the existing `make test SERVICE=search-sync-worker` to confirm no other tests reference `inboxBootstrapStreamConfig`.
3. **`search-sync-worker/main.go`** — no new unit tests needed; the bootstrap loop is exercised by the integration tests in `inbox_integration_test.go` which already build INBOX themselves via `createInboxStream` (Name + Subjects, no Sources). They will continue to pass.
4. **Run integration tests** (`make test-integration SERVICE=search-sync-worker`) to verify the deletion didn't break the full-stack tests.

### Migration

No data migration. Code-only change. Rollout:

1. Land code change. Default `BOOTSTRAP_STREAMS=false` is unchanged for both services in prod.
2. In production, ops/IaC continues to provision INBOX with full federation as before — the change is invisible because the only path that touched federation in app code (`inboxBootstrapStreamConfig`) was gated behind `BOOTSTRAP_STREAMS=true`, which prod doesn't set.
3. In local dev, `inbox-worker` now creates `Name + Subjects` (workable schema) instead of `Name` only. `search-sync-worker` no longer races to overwrite. Local single-site flows continue to work.
4. Local multi-site federation testing — if anyone was using it (no evidence in the repo) — would need to be reconfigured manually or via a separate test harness. Per the historical note, this path was always intended to be temporary.

## Open Questions

None. Decisions confirmed:

- Two-pattern subject schema in `pkg/stream/stream.go` Inbox() stays.
- Federation config moves entirely out of app code (no migration to inbox-worker, no shared helper). It belongs to ops.
- `RemoteSiteIDs` env var is fully removed from app code surface.
- `inboxBootstrapStreamConfig` and its unit-test file are fully deleted, not moved.
