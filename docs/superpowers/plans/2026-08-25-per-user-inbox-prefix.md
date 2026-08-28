> # ⚠️ SUPERSEDED — DO NOT EXECUTE
>
> This plan targets a dedicated `_INBOX.{account}` inbox namespace. That design
> was replaced during implementation: the inbox now lives at
> `chat.user.{account}`, inside the grant the template already carries, and
> **both `_INBOX` grants were deleted**. See §9 of
> `docs/superpowers/specs/2026-08-25-per-user-inbox-prefix-design.md`.
>
> Its checklists still say to set an `_INBOX.` prefix and to add
> `--allow-sub "_INBOX.{{tag(account)}}.>"` to the template. **Following them
> would undo the shipped change and re-open the cross-account inbox hole.**
> Kept only as a record of how the work was sequenced.

# Per-User NATS Inbox Prefix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scope the NATS request/reply inbox to the calling user's account so no client can read or forge another user's RPC replies.

**Architecture:** Clients pass `inboxPrefix: "_INBOX.<account>"` at connect time; the scoped signing-key template grants `--allow-sub "_INBOX.{{tag(account)}}.>"` and no inbox publish at all. Backend responders are unaffected — the `backend` user keeps `--allow-pub ">"`. A new operator-mode integration test executes the real template and proves both the new inbox boundary and the pre-existing `chat.user.*` boundary.

**Tech Stack:** Go 1.25, `nats-io/nats.go` v1.50.0, `nats-io/jwt/v2` v2.8.2, `nats-io/nkeys` v0.4.16, `testcontainers-go` v0.42.0, `nats.ws` 1.30.3, vitest 2.1.

**Spec:** `docs/superpowers/specs/2026-08-25-per-user-inbox-prefix-design.md`

## Global Constraints

- **No new dependencies.** Every library this plan uses is already a direct dependency in `go.mod` or `chat-frontend/package.json`. Do not add any.
- **Go commands go through `make`.** `make test SERVICE=auth-service`, `make test-integration SERVICE=auth-service`, `make lint`, `make fmt`. Never run raw `go test`.
- **Integration tests** carry `//go:build integration`, live in `package main`, and use `-race` (the Makefile handles it).
- **Do not add a `TestMain`** to `auth-service`. One already exists at `auth-service/integration_test.go:76` (`func TestMain(m *testing.M) { testutil.RunTests(m) }`) and a second in the same package will not compile.
- **No `time.Sleep` for synchronization** (CLAUDE.md §3 Concurrency). Use channels with `select` and an explicit timeout.
- **Never edit `mock_store_test.go`** or any generated mock by hand.
- **Frontend tests** run via `npm test` (`vitest run`) from `chat-frontend/`.
- **Exact template strings** — these five subscribe and two publish patterns are the authoritative post-change set, used verbatim in Tasks 3 and 4:
  - sub: `chat.user.{{tag(account)}}.>`, `chat.room.>`, `chat.local.room.>`, `_INBOX.{{tag(account)}}.>`, `chat.user.presence.state.*`
  - pub: `chat.user.{{tag(account)}}.>`, `chat.user.presence.*.query.batch`

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `chat-frontend/src/api/_transport/inbox.js` | **Create.** Single source of the inbox prefix string for JS. | 1 |
| `chat-frontend/src/api/_transport/inbox.test.js` | **Create.** Prefix unit test + `nats.ws` concatenation pin. | 1 |
| `chat-frontend/src/context/NatsContext/NatsContext.jsx` | **Modify.** Pass `inboxPrefix` at connect (196); add status observer (after 214). | 1, 2 |
| `chat-frontend/smoke-test.mjs` | **Modify.** Pass `inboxPrefix` at connect (34). | 1 |
| `chat-frontend/src/context/NatsContext/NatsContext.test.jsx` | **Modify.** Assert connect options and the permission-error path. | 1, 2 |
| `auth-service/permissions_integration_test.go` | **Create.** Operator-mode fixture + template boundary assertions + drift guard. | 3, 4 |
| `docker-local/setup.sh` | **Modify.** Lines 60 and 63 of the signing-key template. | 4 |
| `auth-service/handler.go` | **Modify.** Effective-grants comment block, lines 307-313. | 5 |
| `docs/client-api.md`, `docs/client-api/request-reply.md` | **Modify.** §2.1 table, reply-patterns prose, 111 boilerplate lines. | 5 |

Task order matters: Task 3 defines the Go constants that Task 4's drift guard compares against `setup.sh`.

---

### Task 1: Frontend inbox prefix

**Files:**
- Create: `chat-frontend/src/api/_transport/inbox.js`
- Create: `chat-frontend/src/api/_transport/inbox.test.js`
- Modify: `chat-frontend/src/context/NatsContext/NatsContext.jsx:196-199`
- Modify: `chat-frontend/smoke-test.mjs:34-37`

**Interfaces:**
- Produces: `userInboxPrefix(account: string) => string`, exported from `src/api/_transport/inbox.js`. Task 2 imports it to recognise inbox permission errors.

- [ ] **Step 1: Write the failing test**

Create `chat-frontend/src/api/_transport/inbox.test.js`:

```js
import { describe, it, expect } from 'vitest'
import { createInbox } from 'nats.ws'
import { userInboxPrefix } from './inbox'

describe('userInboxPrefix', () => {
  it('namespaces the inbox under the account', () => {
    expect(userInboxPrefix('alice')).toBe('_INBOX.alice')
  })

  it('produces a prefix nats.ws expands into a matching subject', () => {
    // Pins nats.ws concatenation: createInbox returns `${prefix}.${nuid}`.
    // A future nats.ws bump that changes this breaks the server-side grant
    // `_INBOX.{{tag(account)}}.>`, so it must fail here first.
    const subject = createInbox(userInboxPrefix('alice'))
    expect(subject).toMatch(/^_INBOX\.alice\.[A-Za-z0-9]+$/)
  })
})
```

- [ ] **Step 2: Run the test and confirm it fails**

```bash
cd chat-frontend && npm test -- src/api/_transport/inbox.test.js
```

Expected: FAIL — `Failed to resolve import "./inbox"`.

- [ ] **Step 3: Write the implementation**

Create `chat-frontend/src/api/_transport/inbox.js`:

```js
// The reply namespace a client may subscribe to. The server grants
// `_INBOX.{{tag(account)}}.>` from the scoped signing-key template, so this
// string must match what that substitution produces. Safe because
// auth-service rejects accounts that are not a single valid subject token
// (auth-service/handler.go:189).
export const userInboxPrefix = (account) => `_INBOX.${account}`
```

- [ ] **Step 4: Run the test and confirm it passes**

```bash
cd chat-frontend && npm test -- src/api/_transport/inbox.test.js
```

Expected: PASS, 2 tests.

- [ ] **Step 5: Wire the two connect sites**

In `chat-frontend/src/context/NatsContext/NatsContext.jsx`, add the import beside the existing imports at the top of the file:

```js
import { userInboxPrefix } from '@/api/_transport/inbox'
```

Then change the dial at line 196:

```js
      // 3) Dial the resolved site's NATS.
      const nc = await natsConnect({
        servers: portal.natsUrl,
        authenticator,
        inboxPrefix: userInboxPrefix(userInfo.account),
      })
```

`userInfo` is already destructured at line 180, before this call — no reordering needed.

In `chat-frontend/smoke-test.mjs`, change `connectNats` (line 33-39). `authenticate` returns `{ nkey, natsPublicKey, jwt, user }` where `user` is the auth response's `user` object, so the account is `auth.user.account`. This file is a standalone script run outside the bundler, so inline the string rather than importing the helper:

```js
async function connectNats(auth) {
  const nc = await connect({
    servers: NATS_URL,
    authenticator: jwtAuthenticator(auth.jwt, auth.nkey.getSeed()),
    inboxPrefix: `_INBOX.${auth.user.account}`,
  })
  return nc
}
```

- [ ] **Step 6: Add the connect-options assertion**

The auth fetch mock at `NatsContext.test.jsx:110` returns `user: { account: 'alice' }`, so the expected prefix is `_INBOX.alice`. Add to the existing successful-connect test:

```js
    expect(natsConnect).toHaveBeenCalledWith(
      expect.objectContaining({ inboxPrefix: '_INBOX.alice' }),
    )
```

- [ ] **Step 7: Run the full frontend suite**

```bash
cd chat-frontend && npm test
```

Expected: PASS, no regressions.

- [ ] **Step 8: Commit**

```bash
git add chat-frontend/src/api/_transport/inbox.js \
        chat-frontend/src/api/_transport/inbox.test.js \
        chat-frontend/src/context/NatsContext/NatsContext.jsx \
        chat-frontend/src/context/NatsContext/NatsContext.test.jsx \
        chat-frontend/smoke-test.mjs
git commit -m "feat(chat-frontend): scope the NATS inbox to the user's account"
```

---

### Task 2: Surface inbox permission errors

**Why this is not optional:** on a denied inbox SUB, `nats.ws` (`protocol.js:626-637`) fails in-flight requests, tears down the mux, and recreates it — which is denied again, indefinitely. Without this observer the user sees unexplained failing requests forever. A stale browser tab after the rollout hits exactly this path.

**Files:**
- Modify: `chat-frontend/src/context/NatsContext/NatsContext.jsx` (after the `nc.closed()` block, lines 214-221)
- Modify: `chat-frontend/src/context/NatsContext/NatsContext.test.jsx`

**Interfaces:**
- Consumes: `userInboxPrefix` from Task 1.
- Produces: nothing other tasks depend on.

**Compatibility constraint — read this before writing code.** Every existing connection stub in `NatsContext.test.jsx` is an inline literal such as `{ closed: () => new Promise(() => {}) }` (lines 101, 144-145, 254, 268, 302, 346). **None of them defines `status()`.** An unguarded `nc.status()` throws `TypeError` and breaks all of them. The implementation must therefore feature-detect, exactly as `useJwtRefresh.js:135` does for `reconnect()`. Do not "fix" the existing stubs instead — the guard is the established pattern in this codebase and it also protects against a real connection that has been torn down.

- [ ] **Step 1: Write the failing tests**

Add two tests to `chat-frontend/src/context/NatsContext/NatsContext.test.jsx`, following the existing suite's shape (`renderHook` + `wrapper`, `natsConnect.mockResolvedValue(...)`, then `waitFor`). Read one existing connect test first and mirror how it triggers the connect and reads state off the hook result:

```js
  it('reports an inbox permission violation as a session error', async () => {
    natsConnect.mockReset().mockResolvedValue({
      closed: () => new Promise(() => {}),
      status: async function* () {
        yield { type: 'error', permissionContext: { operation: 'subscription', subject: '_INBOX.alice.abc.*' } }
        await new Promise(() => {})
      },
    })

    // ...drive the same successful connect the neighbouring tests use, then:
    await waitFor(() => {
      expect(result.current.error).toMatch(/permission denied/i)
    })
  })

  it('ignores a permission violation for a subject outside the inbox', async () => {
    natsConnect.mockReset().mockResolvedValue({
      closed: () => new Promise(() => {}),
      status: async function* () {
        yield { type: 'error', permissionContext: { operation: 'publication', subject: 'chat.user.bob.request.x' } }
        await new Promise(() => {})
      },
    })

    // ...drive the connect, then assert no error was set:
    await waitFor(() => expect(result.current.connected).toBe(true))
    expect(result.current.error).toBeNull()
  })
```

The second test matters: without the prefix check, any permission error would be reported as an inbox problem and send users to reload for an unrelated cause.

Adapt `result.current.error` / `result.current.connected` to the property names the existing tests read off the hook — the behaviour under test is fixed, the accessor names are whatever the file already uses.

- [ ] **Step 2: Run the test and confirm it fails**

```bash
cd chat-frontend && npm test -- src/context/NatsContext/NatsContext.test.jsx
```

Expected: FAIL — no error is surfaced, because nothing consumes `status()` today.

- [ ] **Step 3: Write the implementation**

In `NatsContext.jsx`, immediately after the existing `nc.closed().then(...)` block, add:

```js
      // nats.ws recreates the mux after a denied inbox SUB, so a permission
      // error repeats silently forever. Surface it once: the only cause is a
      // JWT whose inbox grant disagrees with this connection's prefix — a
      // stale tab running an old bundle after the prefix rollout.
      const inboxPrefix = userInboxPrefix(userInfo.account)
      if (typeof nc.status === 'function') {
        void (async () => {
          for await (const s of nc.status()) {
            if (myGen !== connectGenRef.current) return
            const subject = s?.permissionContext?.subject
            if (subject && subject.startsWith(`${inboxPrefix}.`)) {
              setError('Reply inbox permission denied — reload the page to reconnect.')
            }
          }
        })()
      }
```

Two guards, both load-bearing:

- `typeof nc.status === 'function'` — matches `useJwtRefresh.js:135`'s treatment of `reconnect()`, and keeps every existing test stub working.
- `myGen !== connectGenRef.current` — mirrors the `closed()` handler directly above: a superseded connection must not clobber the live session's state. It also `return`s out of the loop, so the iteration cannot leak across reconnects.

- [ ] **Step 4: Run the test and confirm it passes**

```bash
cd chat-frontend && npm test -- src/context/NatsContext/NatsContext.test.jsx
```

Expected: PASS.

- [ ] **Step 5: Run the full frontend suite**

```bash
cd chat-frontend && npm test
```

Expected: PASS, no regressions. This is the step that proves the `typeof nc.status === 'function'` guard holds: the six pre-existing connect tests use stubs without `status()`, and any failure here means the guard was dropped or misplaced.

- [ ] **Step 6: Commit**

```bash
git add chat-frontend/src/context/NatsContext/NatsContext.jsx \
        chat-frontend/src/context/NatsContext/NatsContext.test.jsx
git commit -m "feat(chat-frontend): surface inbox permission violations as a session error"
```

---

### Task 3: Operator-mode template test

This task builds a NATS server running the **real scoped signing-key template** and asserts the permission boundary. The red phase is genuine and valuable: the fixture starts with today's `_INBOX.>` grant, so the cross-user assertion fails and demonstrates the actual vulnerability before the fix.

**Files:**
- Create: `auth-service/permissions_integration_test.go`

**Interfaces:**
- Produces, for Task 4:
  - `var scopedUserSub []string` — the five subscribe patterns
  - `var scopedUserPub []string` — the two publish patterns
  - `func startOperatorNATS(t *testing.T) *operatorNATS`
  - `func (o *operatorNATS) connectScoped(t *testing.T, account string) *nats.Conn`
  - `func (o *operatorNATS) connectBackend(t *testing.T) *nats.Conn`

- [ ] **Step 1: Write the fixture and the failing boundary test**

Create `auth-service/permissions_integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/hmchangw/chat/pkg/testutil/testimages"
)

// scopedUserSub and scopedUserPub mirror the --allow-sub / --allow-pub flags
// of the scoped signing key in docker-local/setup.sh. TestSetupScriptMatchesTemplate
// fails if the two drift.
//
// RED PHASE: this starts as the CURRENT template (_INBOX.>). Step 3 narrows it.
var scopedUserSub = []string{
	"chat.user.{{tag(account)}}.>",
	"chat.room.>",
	"chat.local.room.>",
	"_INBOX.>",
	"chat.user.presence.state.*",
}

var scopedUserPub = []string{
	"chat.user.{{tag(account)}}.>",
	"_INBOX.>",
	"chat.user.presence.*.query.batch",
}

// operatorNATS is a nats-server in operator mode whose account carries the
// scoped signing key under test. Scoped user JWTs are minted per account tag,
// exactly as auth-service does (handler.go:318-323).
type operatorNATS struct {
	url       string
	accKP     nkeys.KeyPair
	signingKP nkeys.KeyPair
	accPub    string
}

func startOperatorNATS(t *testing.T) *operatorNATS {
	t.Helper()
	ctx := context.Background()

	opKP, err := nkeys.CreateOperator()
	require.NoError(t, err)
	opPub, err := opKP.PublicKey()
	require.NoError(t, err)

	accKP, err := nkeys.CreateAccount()
	require.NoError(t, err)
	accPub, err := accKP.PublicKey()
	require.NoError(t, err)

	signingKP, err := nkeys.CreateAccount()
	require.NoError(t, err)
	signingPub, err := signingKP.PublicKey()
	require.NoError(t, err)

	opClaims := jwt.NewOperatorClaims(opPub)
	opClaims.Name = "test-operator"
	opJWT, err := opClaims.Encode(opKP)
	require.NoError(t, err)

	accClaims := jwt.NewAccountClaims(accPub)
	accClaims.Name = "chatapp"
	scope := jwt.NewUserScope()
	scope.Key = signingPub
	scope.Role = "scoped_user"
	scope.Template.Sub.Allow.Add(scopedUserSub...)
	scope.Template.Pub.Allow.Add(scopedUserPub...)
	accClaims.SigningKeys.AddScopedSigner(scope)
	accJWT, err := accClaims.Encode(opKP)
	require.NoError(t, err)

	conf := fmt.Sprintf("operator: %s\nresolver: MEMORY\nresolver_preload: {\n  %s: %s\n}\n",
		opJWT, accPub, accJWT)
	confPath := filepath.Join(t.TempDir(), "nats.conf")
	require.NoError(t, os.WriteFile(confPath, []byte(conf), 0o600))

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        testimages.NATS,
			ExposedPorts: []string{"4222/tcp"},
			Files: []testcontainers.ContainerFile{{
				HostFilePath:      confPath,
				ContainerFilePath: "/etc/nats/nats.conf",
				FileMode:          0o644,
			}},
			Cmd:        []string{"-c", "/etc/nats/nats.conf"},
			WaitingFor: wait.ForLog("Server is ready").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "4222/tcp")
	require.NoError(t, err)

	return &operatorNATS{
		url:       fmt.Sprintf("nats://%s:%s", host, port.Port()),
		accKP:     accKP,
		signingKP: signingKP,
		accPub:    accPub,
	}
}

// connectScoped mints a scoped user JWT tagged with account and dials with it.
// PermissionErrOnSubscribe makes a denied SubscribeSync return
// nats.ErrPermissionViolation synchronously instead of only reaching the async
// error handler.
func (o *operatorNATS) connectScoped(t *testing.T, account string) *nats.Conn {
	t.Helper()
	userKP, err := nkeys.CreateUser()
	require.NoError(t, err)
	userPub, err := userKP.PublicKey()
	require.NoError(t, err)

	uc := jwt.NewUserClaims(userPub)
	uc.IssuerAccount = o.accPub
	uc.Expires = time.Now().Add(time.Hour).Unix()
	uc.Tags.Add("account:" + account)
	uc.SetScoped(true)
	userJWT, err := uc.Encode(o.signingKP)
	require.NoError(t, err)

	seed, err := userKP.Seed()
	require.NoError(t, err)

	nc, err := nats.Connect(o.url,
		nats.UserJWTAndSeed(userJWT, string(seed)),
		nats.PermissionErrOnSubscribe(true),
	)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

// connectBackend dials as the unscoped backend user — signed by the account
// key itself, so it carries no permission restrictions, exactly like the
// `backend` user in setup.sh:92.
func (o *operatorNATS) connectBackend(t *testing.T) *nats.Conn {
	t.Helper()
	userKP, err := nkeys.CreateUser()
	require.NoError(t, err)
	userPub, err := userKP.PublicKey()
	require.NoError(t, err)

	uc := jwt.NewUserClaims(userPub)
	uc.Expires = time.Now().Add(time.Hour).Unix()
	userJWT, err := uc.Encode(o.accKP)
	require.NoError(t, err)

	seed, err := userKP.Seed()
	require.NoError(t, err)

	nc, err := nats.Connect(o.url, nats.UserJWTAndSeed(userJWT, string(seed)))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

// TestScopedTemplate_InboxIsolation is the boundary this change exists for:
// one user must not be able to read another user's replies.
func TestScopedTemplate_InboxIsolation(t *testing.T) {
	o := startOperatorNATS(t)
	bob := o.connectScoped(t, "bob")

	_, err := bob.SubscribeSync("_INBOX.alice.>")
	require.ErrorIs(t, err, nats.ErrPermissionViolation,
		"bob must not be able to subscribe to alice's reply inbox")
}
```

- [ ] **Step 2: Run it and confirm it fails**

```bash
make test-integration SERVICE=auth-service
```

Expected: FAIL on `TestScopedTemplate_InboxIsolation` — bob's subscribe **succeeds** under the current `_INBOX.>` grant. This failure is the vulnerability, reproduced.

- [ ] **Step 3: Narrow the template**

In the same file, change the two constants to the post-change set:

```go
var scopedUserSub = []string{
	"chat.user.{{tag(account)}}.>",
	"chat.room.>",
	"chat.local.room.>",
	"_INBOX.{{tag(account)}}.>",
	"chat.user.presence.state.*",
}

var scopedUserPub = []string{
	"chat.user.{{tag(account)}}.>",
	"chat.user.presence.*.query.batch",
}
```

Delete the `RED PHASE` line from the comment above them.

- [ ] **Step 4: Run it and confirm it passes**

```bash
make test-integration SERVICE=auth-service
```

Expected: PASS.

- [ ] **Step 5: Add the remaining boundary assertions**

Append to the same file. The round-trip test is what proves a subscribe-only grant is sufficient — if it fails, the spec's fallback applies (add `_INBOX.{{tag(account)}}.>` to `scopedUserPub`) and that decision gets recorded in the commit message.

```go
// TestScopedTemplate_RequestReplyRoundTrip proves a subscribe-only inbox grant
// is sufficient: the requester never publishes to its own inbox, it only sets
// the reply-to, and the responder publishes there.
func TestScopedTemplate_RequestReplyRoundTrip(t *testing.T) {
	o := startOperatorNATS(t)
	backend := o.connectBackend(t)
	alice := o.connectScoped(t, "alice")

	_, err := backend.Subscribe("chat.user.alice.request.ping", func(m *nats.Msg) {
		_ = m.Respond([]byte("pong"))
	})
	require.NoError(t, err)
	require.NoError(t, backend.Flush())

	reply, err := alice.Request("chat.user.alice.request.ping", []byte("ping"), 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, "pong", string(reply.Data))
}

// TestScopedTemplate_InboxPublishDenied proves the forged-reply vector is gone:
// no client may publish to any inbox, including its own.
func TestScopedTemplate_InboxPublishDenied(t *testing.T) {
	o := startOperatorNATS(t)
	alice := o.connectScoped(t, "alice")

	permErr := make(chan error, 1)
	alice.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		select {
		case permErr <- err:
		default:
		}
	})

	require.NoError(t, alice.Publish("_INBOX.alice.forged", []byte("spoof")))
	require.NoError(t, alice.Flush())

	select {
	case err := <-permErr:
		require.ErrorIs(t, err, nats.ErrPermissionViolation)
	case <-time.After(5 * time.Second):
		t.Fatal("expected a permission violation for publishing to an inbox")
	}
}

// TestScopedTemplate_UserNamespace covers the grant that predates this change
// and was never executed by any test.
func TestScopedTemplate_UserNamespace(t *testing.T) {
	o := startOperatorNATS(t)

	t.Run("own namespace allowed", func(t *testing.T) {
		alice := o.connectScoped(t, "alice")
		_, err := alice.SubscribeSync("chat.user.alice.event")
		require.NoError(t, err)
		_, err = alice.SubscribeSync("chat.room.r1.event")
		require.NoError(t, err)
	})

	t.Run("subscribing to another user is denied", func(t *testing.T) {
		alice := o.connectScoped(t, "alice")
		_, err := alice.SubscribeSync("chat.user.bob.>")
		require.ErrorIs(t, err, nats.ErrPermissionViolation)
	})

	t.Run("publishing as another user is denied", func(t *testing.T) {
		alice := o.connectScoped(t, "alice")
		permErr := make(chan error, 1)
		alice.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			select {
			case permErr <- err:
			default:
			}
		})

		require.NoError(t, alice.Publish("chat.user.bob.request.room.s1.create", []byte("{}")))
		require.NoError(t, alice.Flush())

		select {
		case err := <-permErr:
			require.ErrorIs(t, err, nats.ErrPermissionViolation)
		case <-time.After(5 * time.Second):
			t.Fatal("expected a permission violation for publishing as another user")
		}
	})
}
```

- [ ] **Step 6: Run the whole integration suite**

```bash
make test-integration SERVICE=auth-service
```

Expected: PASS, all five tests.

If `TestScopedTemplate_RequestReplyRoundTrip` fails with a permission violation, do **not** delete the test. Add `"_INBOX.{{tag(account)}}.>"` to `scopedUserPub`, re-run, and note in the commit message that publish had to be retained and why.

- [ ] **Step 7: Lint and commit**

```bash
make lint
git add auth-service/permissions_integration_test.go
git commit -m "test(auth-service): assert the scoped signing-key permission boundary"
```

---

### Task 4: Narrow the dev template

**Files:**
- Modify: `docker-local/setup.sh:60,63`
- Modify: `auth-service/permissions_integration_test.go` (add the drift guard)

**Interfaces:**
- Consumes: `scopedUserSub`, `scopedUserPub` from Task 3.

- [ ] **Step 1: Write the failing drift guard**

Append to `auth-service/permissions_integration_test.go`:

```go
// TestSetupScriptMatchesTemplate pins docker-local/setup.sh to the permission
// set the boundary tests execute. The prod template is the platform team's and
// lives outside this repo — this guard covers the dev mirror only.
func TestSetupScriptMatchesTemplate(t *testing.T) {
	raw, err := os.ReadFile("../docker-local/setup.sh")
	require.NoError(t, err)
	script := string(raw)

	for _, subj := range scopedUserSub {
		require.Contains(t, script, fmt.Sprintf("--allow-sub %q", subj),
			"setup.sh is missing a subscribe grant the tests assert")
	}
	for _, subj := range scopedUserPub {
		require.Contains(t, script, fmt.Sprintf("--allow-pub %q", subj),
			"setup.sh is missing a publish grant the tests assert")
	}
	require.NotContains(t, script, `--allow-sub "_INBOX.>"`,
		"the wildcard inbox subscribe grant must be gone")
	require.NotContains(t, script, `--allow-pub "_INBOX.>"`,
		"the wildcard inbox publish grant must be gone")
}
```

- [ ] **Step 2: Run it and confirm it fails**

```bash
make test-integration SERVICE=auth-service
```

Expected: FAIL — `setup.sh` still carries both `_INBOX.>` lines and lacks the scoped subscribe grant.

- [ ] **Step 3: Edit the template**

In `docker-local/setup.sh`, replace line 60 and delete line 63:

```diff
       --allow-sub "chat.local.room.>" \
-      --allow-sub "_INBOX.>" \
+      --allow-sub "_INBOX.{{tag(account)}}.>" \
       --allow-sub "chat.user.presence.state.*" \
       --allow-pub "chat.user.{{tag(account)}}.>" \
-      --allow-pub "_INBOX.>" \
       --allow-pub "chat.user.presence.*.query.batch" \
```

Update the comment above the block (lines 53-55) to record why publish is absent:

```bash
    # Scoped signing key that auth-service uses to sign user JWTs. The role
    # template mirrors the grants auth-service used to inline per JWT, keyed
    # off the account:<account> tag every user JWT now carries. The inbox is
    # per-account and subscribe-only: a client sets its reply-to but never
    # publishes to an inbox, so no client can read or forge another's replies.
```

- [ ] **Step 4: Run it and confirm it passes**

```bash
make test-integration SERVICE=auth-service
```

Expected: PASS, all six tests.

- [ ] **Step 5: Verify against a real local stack**

```bash
cd docker-local && ./setup.sh && cd .. && make deps-up
```

Then run the frontend against it and confirm a login plus one RPC (opening a room) works end to end. This is the only step that exercises the regenerated `nsc` template rather than the Go fixture's reconstruction of it, so it is not optional. If `setup.sh` refuses the templated subject, capture the error and stop — that would mean the installed `nsc` validates templates differently than the server, and the plan needs revisiting before anything ships.

- [ ] **Step 6: Commit**

```bash
git add docker-local/setup.sh auth-service/permissions_integration_test.go
git commit -m "feat(auth): scope the client inbox grant to the caller's account"
```

---

### Task 5: Documentation

**Files:**
- Modify: `auth-service/handler.go:307-313`
- Modify: `docs/client-api.md` (71 `_INBOX` occurrences)
- Modify: `docs/client-api/request-reply.md` (45 occurrences)

- [ ] **Step 1: Update the effective-grants comment**

In `auth-service/handler.go`, replace lines 312-313:

```go
//	Pub allow: chat.user.{account}.>, chat.user.presence.*.query.batch (+allow-pub-response)
//	Sub allow: chat.user.{account}.>, chat.room.>, chat.local.room.>, _INBOX.{account}.>, chat.user.presence.state.*
```

Add below the existing list, before the closing paragraph:

```go
// The inbox is per-account and subscribe-only: clients set a reply-to but never
// publish to an inbox, so no client can read or forge another user's replies.
// auth-service/permissions_integration_test.go executes this template.
```

- [ ] **Step 2: Update the §2.1 permission table**

In `docs/client-api.md`, change the subscribe row (line 173) and delete the publish row (line 168):

```markdown
| Subscribe | `_INBOX.{account}.>` | Required to receive replies to client-issued requests. The client sets `inboxPrefix` to `_INBOX.{account}` at connect time; the grant is per-account, so no client can read another user's replies. Publishing to an inbox is not granted — a client sets a reply-to but never publishes to one. |
```

- [ ] **Step 3: Update the reply-patterns prose**

In `docs/client-api.md` line 150:

```markdown
- **Standard NATS request/reply** — the NATS client library auto-generates a reply subject under `_INBOX.{account}.>` (from the connection's `inboxPrefix`, see §2.1) and routes the reply back to the caller. Used by every method in §3.
```

- [ ] **Step 4: Replace the boilerplate reply-subject lines**

```bash
sed -i 's/auto-generated `_INBOX\.>`/auto-generated `_INBOX.{account}.>`/g' \
  docs/client-api.md docs/client-api/request-reply.md
```

- [ ] **Step 5: Review what the script missed**

```bash
grep -n '_INBOX' docs/client-api.md docs/client-api/request-reply.md | grep -v '_INBOX\.{account}\.>'
```

Expected: only the §2.1 table row and the reply-patterns line from Steps 2-3, both already correct in their new form. Hand-edit anything else the grep surfaces. `docs/client-api/events.md` contains no `_INBOX` and must not be touched.

- [ ] **Step 6: Verify no bare inbox reference survives**

```bash
grep -rn '_INBOX\.>' docs/ && echo "FAIL: bare _INBOX.> still present" || echo "OK"
```

Expected: `OK` — every documented inbox reference now carries the account token.

- [ ] **Step 7: Lint and commit**

```bash
make lint
git add auth-service/handler.go docs/client-api.md docs/client-api/request-reply.md
git commit -m "docs: document the per-account request/reply inbox"
```

---

## Rollout Note (not a code task)

Deploy order is **clients first, then the template** — a new client works against the old wildcard grant, but an old client breaks against the new one. See §7 of the spec for the compatibility matrix. The prod template is the platform team's copy of `setup.sh:56-64` and is edited by them, in that order, after the client build ships.
