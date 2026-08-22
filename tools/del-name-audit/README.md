# del-name-audit

Read-only probe answering one question:

> Can `user-service` drop its `rooms` `$lookup` from the subscription count/list
> paths and filter on the subscription's own denormalized `name` instead?

`CountActiveSubscriptions` joins `subscriptions` → `rooms` for exactly one field:
`room.name`, to exclude soft-deleted (`Del-`) rooms. `subscriptions.name` is
already a denormalized copy of the room name, maintained by
`UpdateSubscriptionNamesForRoom` (room-worker for in-stack renames, inbox-worker
for oplog/cross-site renames). If the copy is faithful, the join is unnecessary.

Faithful is a data question, not a code question — hence this tool.

## What it checks

Both directions, because either one alone can be misleading:

| Direction | Question | Failure means |
|-----------|----------|---------------|
| Forward | Does every subscription of a `Del-` room carry the marker? | A `disagree` — the filter would **keep** a deleted room |
| Reverse | Does any `Del-`-named subscription point at a live room? | A `falsePositive` — the filter would **drop** a live room |

Results split by `roomType`. That split matters: a subscription's `name` is the
room name for `channel`, but the **counterpart account** for `dm` and the bot
name for `botDM`. A DM-only divergence would otherwise average away against the
channel totals.

A `Del-`-named subscription whose room has no local doc counts as
**unverifiable**, not a false positive — it is cross-site, and cross-site rows
are excluded under both the join and the name filter.

## Running it

```sh
MONGO_URI='mongodb://…' MONGO_DB=chat make audit-del-names
```

Against a secondary or a restored dump; `ConnectRead` (`secondaryPreferred`)
keeps the scan off the primary. Nothing is written.

Flags via `AUDIT_FLAGS`:

| Flag | Default | Meaning |
|------|---------|---------|
| `--rooms N` | `0` (all) | Cap the `Del-` rooms scanned |
| `--subs N` | `0` (all) | Cap the `Del-`-named subscriptions scanned |
| `--samples N` | `20` | Counter-examples to print |
| `--json` | off | Emit the report as JSON |

```sh
AUDIT_FLAGS='--rooms 5000 --json' make audit-del-names
```

Env: `MONGO_URI`, `MONGO_DB` (default `chat`), `MONGO_USERNAME`,
`MONGO_PASSWORD`, `AUDIT_TIMEOUT` (default `10m`).

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | **SAFE** — no disagreements, no false positives |
| `1` | **NOT SAFE** — keep the `$lookup`; counter-examples are printed |
| `2` | Config, connection, or query error |

Run it per site. The subscription set is site-scoped, and a rename federates
through the OUTBOX ordered lane, so one site agreeing does not prove another does.

## Caveat

The verdict describes the data **as of the scan**. It is evidence for a decision,
not a constraint that keeps holding — if a writer can still introduce a `Del-`
rename that does not fan out to subscriptions, a passing run today can be stale
tomorrow. Re-run it after any change to the rename fan-out path.
