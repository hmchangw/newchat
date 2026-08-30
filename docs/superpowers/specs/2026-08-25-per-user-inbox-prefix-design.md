# Per-User NATS Inbox Prefix for Request/Reply

**Date:** 2026-08-25
**Status:** Superseded in part — see §9. The problem statement and the testing
approach stand; the chosen subject scheme changed from a dedicated `_INBOX`
namespace to the client's own `chat.user.{account}` namespace.
**Related:** `docker-local/setup.sh:56-64` (scoped signing key template),
`auth-service/handler.go:307-313` (effective-grants comment),
`docs/client-api.md` §2.1 (published permission table)

## 1. Problem

Every client JWT minted by `auth-service` is scoped by one signing-key template
(`docker-local/setup.sh:56-64`). Its user-scoped grants are already per-account —
`chat.user.{{tag(account)}}.>` — but the inbox grants are not:

```
--allow-sub "_INBOX.>"
--allow-pub "_INBOX.>"
```

`_INBOX` is not special to nats-server; it is a client-library convention over an
ordinary subject. A wildcard grant on it therefore hands every authenticated user
the whole reply namespace, in both directions:

- **Read.** Any user may `SUB _INBOX.>` and receive every other user's RPC replies —
  `subscription.list`, room metadata, history pages, and the `key.get` responses that
  carry room key material.
- **Write.** Any user may publish to another user's live inbox subject and land a
  forged reply. `nc.request` resolves on the first message that arrives on the
  subject, so a spoofed reply wins if it beats the real responder.

Both are invisible in normal operation: nothing fails, no error is logged, and the
grant looks correct next to the per-account grants beside it.

## 2. Scope

**In:**

- `docker-local/setup.sh` — the two inbox lines of the signing-key template.
- `chat-frontend` — a prefix helper plus its two connect sites.
- `auth-service` — a new operator-mode integration test asserting the template.
- `docs/client-api.md` and `docs/client-api/request-reply.md`.

**Out:**

- Backend services. The `backend` user keeps `--allow-pub ">"` (`setup.sh:92`), so
  every responder replies exactly as it does today. No service code changes.
- `--allow-pub-response` in the template. It grants a user a one-shot reply
  permission for requests it receives; clients in this system do not serve requests,
  so it appears vestigial. Removing it is a separate change with its own blast
  radius.
- The prod template itself, which the platform team owns. This spec covers the dev
  mirror and the clients; the prod edit is a coordinated manual step (§7).

## 3. Subject scheme

Prefix: `_INBOX.<account>`. Generated subjects: `_INBOX.<account>.<nuid>.<token>`,
matched by `_INBOX.<account>.>` at any depth.

Both client libraries were verified against source to produce that shape from the
same input string:

- `nats.go` v1.50.0 — `CustomInboxPrefix` (`nats.go:1502-1510`) rejects only
  wildcards and a trailing dot, so dots are legal; `Conn.NewInbox`
  (`nats.go:4611-4621`) writes `prefix + "." + nuid`; `initNewResp`
  (`nats.go:4624-4627`) subscribes the mux at `<inbox>.*`.
- `nats.ws` 1.30.3 — `createInbox` (`core.js:231-243`) returns
  `` `${prefix}.${nuid}` `` and validates each dot-separated token against wildcards;
  `MuxSubscription.init` (`muxsubscription.js:27-30`) appends the trailing dot and
  per-request token.

`_INBOX.{{tag(account)}}.>` is whole-token substitution — the same form as
`chat.user.{{tag(account)}}.>`, which already runs in production. The design relies
on no unverified server behavior.

Client and template cannot derive different strings: `auth-service/handler.go:189`
rejects any account failing `subject.IsValidAccountToken`
(`pkg/subject/subject.go:60-70`), which bars `.`, `*`, `>`, whitespace, and control
characters. An account is always exactly one safe subject token.

## 4. Server template

```diff
-      --allow-sub "_INBOX.>" \
+      --allow-sub "_INBOX.{{tag(account)}}.>" \
-      --allow-pub "_INBOX.>" \
```

The grant becomes **subscribe-only**. In the muxed request/reply path a client never
publishes to an inbox: it publishes the request to `chat.user.…` and carries the
inbox as the reply-to field, and setting a reply-to requires no publish permission —
only the responder needs one. Dropping publish is therefore both correct and
strictly tighter than narrowing it, and it removes the forged-reply vector outright
rather than reducing it to self-spoofing.

This is deliberately past the minimum, so it is evidence-gated: the §6 fixture
asserts a full round-trip under a subscribe-only grant. If some path does need
publish — a JetStream call from the browser is the only plausible candidate — the
fallback is `--allow-pub "_INBOX.{{tag(account)}}.>"`, which still closes both
original leaks. That decision is made from a failing test, not pre-emptively.

## 5. Client changes

```js
// chat-frontend/src/api/_transport/inbox.js
export const userInboxPrefix = (account) => `_INBOX.${account}`
```

Two call sites:

- `chat-frontend/src/context/NatsContext/NatsContext.jsx:196` — add
  `inboxPrefix: userInboxPrefix(userInfo.account)` to the `natsConnect` options.
  `userInfo` is destructured from the auth response at line 180, before the dial, so
  no restructuring is needed.
- `chat-frontend/smoke-test.mjs:34` — the same option on its `connect` call.

No Go production client is affected. `tools/loadgen` connects with `backend.creds`
(`daily_pool.go:183-186`); `maxrps_login.go:167` mints a JWT to measure login
throughput but never dials NATS with it; bots authenticate over HTTP through
`pkg/botauth`. The prefix string is therefore needed in JS only — nothing is added
to `pkg/subject`, where it would be an exported function with no production caller.

### 5.1 Failure handling

`NatsContext` observes only `nc.closed()` today. Add a `nc.status()` observer that
calls the existing `setError` (declared line 74, used at line 219) when a status
carries a `permissionContext` whose subject falls under the inbox prefix.

This is load-bearing rather than cosmetic. On a denied inbox SUB, `nats.ws`
(`protocol.js:626-637`) fails in-flight requests, then tears down and recreates the
mux — which is denied again, indefinitely. Without an observer the user sees an
unexplained trickle of failing requests. With it, one clear message — for a
prefix/template mismatch, a partially-rolled-out or lagging production template, or
a JWT whose account tag disagrees with the connection's prefix. (A tab still running
the *old* bundle predates this observer entirely, so it cannot be the population
this catches — see §7's rollout consequences for that case.)

## 6. Testing

### 6.1 Operator-mode fixture

Nothing in the repo runs nats-server in operator/JWT mode, so no test can currently
execute the template — the pre-existing `chat.user.{{tag(account)}}.>` grant is
equally unverified. `auth-service/integration_test.go:73` asserts only that the
minted JWT is scoped and carries no inline permissions; it never starts a server.

A new `auth-service/permissions_integration_test.go` (build tag `integration`)
closes that gap for both grants. It builds everything in Go — no `nsc`, no shell:
operator/account/user NKeys via `nkeys`, an account JWT whose signing key carries a
`jwt.UserScope` with the permission template, a conf using `resolver: MEMORY` with
`resolver_preload`, and a container started on that conf. Scoped user JWTs are then
minted per account tag.

Per CLAUDE.md this is an inline `testcontainers.GenericContainer`, not a
`pkg/testutil` helper: the shared `testutil.NATS(t)` cannot accommodate a bespoke
operator-mode config, and only one package needs it. The container reference is
stored and `t.Cleanup(container.Terminate)` registered. Promote to `pkg/testutil`
with the standard `Xxx(t)` / `EnsureXxx()` / `TerminateXxx()` shape if a second
package ever needs it.

### 6.2 Assertions

Table-driven:

| Principal | Action | Expect |
|---|---|---|
| alice | full request/reply round-trip via a responder | succeeds — proves subscribe-only suffices |
| alice | SUB `_INBOX.bob.>` | denied |
| alice | PUB `_INBOX.alice.…` | denied — proves publish is gone |
| alice | PUB and SUB `chat.user.bob.>` | denied — the pre-existing gap |
| alice | PUB and SUB `chat.user.alice.>`, SUB `chat.room.>` | allowed — regression guard on the rest of the template |

nats.go delivers permission violations **asynchronously** through the connection's
error handler, not as a `Subscribe()` return value. Denials are asserted via an
error channel and a `select` with timeout — never `time.Sleep`, per CLAUDE.md
§3 Concurrency.

### 6.3 Drift guard

The permission set will exist in three places: the Go fixture, `setup.sh`, and the
platform team's prod template. The third is outside the repo, so single-sourcing is
impossible. The available mitigation is a test that reads `docker-local/setup.sh`
and asserts its `--allow-sub` / `--allow-pub` lines match the fixture's set. That
catches dev↔test drift cheaply and leaves prod as a documented human step (§7).

### 6.4 Frontend tests

- A unit test for `userInboxPrefix`.
- A test asserting `createInbox('_INBOX.alice')` matches
  `/^_INBOX\.alice\.[A-Za-z0-9]+$/`, pinning the concatenation semantics verified in
  §3 so a future `nats.ws` bump cannot silently change them.

## 7. Rollout

Atomic, in the sense that matters: the wildcard grant is removed in a single step,
with no period in which both the old and the new grant are active.

The two edits are not symmetric, however, and the order follows from that:

|  | old template (`_INBOX.>`) | new template (`_INBOX.{{tag(account)}}.>`) |
|---|---|---|
| **old client** (default `_INBOX.` prefix) | works | **broken** |
| **new client** (`_INBOX.<account>` prefix) | works — `_INBOX.>` covers it | works |

Only one cell fails, so **deploy the clients first, then apply the template**. A new
client against the old template is fully functional, which means the client deploy
can land well ahead of the template edit and be verified in isolation. Reversing the
order breaks every client for the length of the window and buys nothing.

Consequences, accepted:

- A browser tab still running the **old** bundle at the moment of the template edit
  breaks until reload; its connection holds the default `_INBOX.` prefix, which the
  new template denies. Client-first ordering shrinks this to tabs that have not
  picked up the new bundle, rather than all tabs. That old bundle predates §5.1's
  observer, so it surfaces only as the pre-existing trickle of failing requests, not
  a clear message; the support answer is still "reload the tab".
- Rollback of the template alone is safe and sufficient: new clients keep working
  against the restored wildcard, so the client deploy does not need to be reverted
  with it.

## 8. Documentation

> **Superseded by §9.** The subject form below (`_INBOX.{account}.>`) is not
> what shipped — replies ride `chat.user.{account}` and both `_INBOX` grants
> were removed. The list of files to update is still accurate; the subject in
> it is not.


- `docs/client-api.md` §2.1 — the subscribe row becomes `_INBOX.{account}.>`; the
  publish row is deleted; the "Reply patterns" paragraph (line 150) is reworded.
- The `**Reply subject:** auto-generated \`_INBOX.>\`` boilerplate — 111 of the 116
  `_INBOX` occurrences across `docs/client-api.md` (71) and
  `docs/client-api/request-reply.md` (45). Scripted replace, then manual review of
  the ~5 non-boilerplate mentions. `docs/client-api/events.md` has none.
- `auth-service/handler.go:307-313` — the effective-grants comment block.


## 9. Amendment — inbox moved into the user namespace

The prefix is `chat.user.{account}`, not `_INBOX.{account}`. Replies land on
`chat.user.<account>.<nuid>.<token>`.

**Why.** `chat.user.{{tag(account)}}.>` is already granted, in production, today.
Putting replies inside it means a client can adopt the new prefix against an
*unchanged* server, which removes the coupling §7 was built to manage: clients
migrate on their own schedule, and deleting the now-dead `_INBOX` grants becomes
a cleanup with no client in the loop. The compatibility matrix in §7 still
describes what happens when those grants are finally removed, but the migration
no longer has to be a single window.

**What changed as a result.**

- The template gains nothing and loses two lines: both `_INBOX` grants are
  deleted outright (§4 is superseded).
- §6.2's `InboxPublishDenied` is replaced by `CrossUserInboxPublishDenied`.
  Publishing to one's *own* inbox is now permitted, because the user namespace
  is granted for publish as well as subscribe. Cross-user forgery stays blocked:
  bob's publish grant reaches only `chat.user.bob.>`.
- That give-back is permanent by construction. A deny rule needs a subject to
  match on, and a bare `chat.user.{account}` prefix produces an opaque nuid with
  no distinct segment. A `chat.user.{account}.inbox` prefix would have preserved
  the option; it was considered and declined in favour of the simpler subject.
- Reply traffic now overlaps the `chat.user.{account}.>` wildcard that
  `docs/client-api.md` recommends as a baseline subscription. The NATS client
  library routes replies to the awaiting request rather than to that
  subscription, but any handler on the wildcard must ignore subjects it does not
  recognise. Documented at both recommendation sites.

## 10. Amendment — what this branch ships

This branch ships the client change (§5) and the dev template (§4 as amended by
§9): the frontend sets `inboxPrefix` to `chat.user.{account}` and surfaces a
permission error on that subscription, and `docker-local/setup.sh` drops both
`_INBOX` grants from the `scoped_user` role.

Deferred, each independently:

- **The prod template.** The platform team's copy of the same
  `nsc edit signing-key` invocation lives outside this repo. Until it is
  applied, prod still carries the wildcard grants and the exposure in §1
  remains open there — clients merely stop relying on them. §9 is what makes
  that ordering safe: the new prefix works against an unchanged server.
- **Account canonicalisation.** auth-service returns `user.account` as given,
  which need not equal the `account:` tag the `{{tag(account)}}` grant is
  evaluated against (`jwt.TagList.Add` lowercases and trims). Every
  user-namespace subject the frontend builds already depends on those matching,
  so this change introduces no new failure mode — but the invariant is still
  unenforced.
- **The tests in §6.** The operator-mode boundary tests and the setup.sh drift
  guard are Go, and land with the auth-service change. Nothing in CI currently
  pins the template that §4 describes.
