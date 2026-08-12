# Priority-Contact Presence Gating (Spec 3 of 3)

Issue #221. Spec 1 (`2026-08-08-priority-contacts-storage-api-design.md`) stored the
settings. Spec 2 (`2026-08-10-notification-settings-enforcement-design.md`) made
`notification-worker` enforce three of them. This spec changes one thing today —
presence-based suppression gains a priority-contact exemption — and wires the rule
that will stop manual do-not-disturb borrowing the in-call checkbox as inert
predicates, for the presence side to implement.

The issue calls the component "push-notification-worker". That is
`notification-worker`: its push path (`mobileEmitter` → `PUSH-NOTIFICATION` →
`push-notification-service` → APNs/FCM) *is* the mobile lane, so "Desktop settings
that sync to Mobile" means "the stored user settings gate this pipeline". No
separate desktop/mobile settings store exists or is introduced.

## Scope

One function. `shouldPush` in `notification-worker/presence.go` is the only
behavioural change. The candidate loop, the settings snapshotter, presence
fetching, survivor sorting, batching and the `{messageID}-b{N}` dedup ID are all
untouched — as in Spec 2, this changes only which accounts reach `survivors`.

### Out of scope

- **New config.** `USER_SETTINGS_ENABLED=false` already reverts the entire gate to
  pre-enforcement behaviour, including everything here, so a second kill switch
  would only add a state nobody tests.
- **New wire surface.** No RPC, model field or event changes, so the derived views
  (`docs/client-api/request-reply.md`, `docs/client-api/events.md`) are untouched
  and the CLAUDE.md same-PR rule for them is not triggered.
- **`showPreviewsInNotifications`.** Still shapes the push *body*, not whether one
  is sent. Out of scope for the same reason Spec 2 gave.
- **Per-room notification rules** beyond what the pipeline already applies. See
  "Room rules are absolute" below.

## Do-not-disturb and presenting are not ours to derive

The issue names three presence conditions — `Do not disturb`, `presenting`, and
`In a call`. Only the third exists in our vocabulary today. `busy` is a manual
override and `Presenting` is currently folded into `in-call` by
`user-presence-service/sync/reconcile.go`.

**Decision: do not infer either one. Stub both predicates.**

An earlier draft of this spec mapped `Do not disturb → busy` and
`presenting → in-call`. That was wrong. Deriving DND from `busy` asserts a
semantic equivalence this service has no authority to declare, and the same for
presenting. Those statuses are owned by the presence side of the system, which
will ship them; this worker's job is to gate on them, not to guess at them.

So `notification-worker` declares the two predicates it needs and leaves them
inert:

```go
var (
	isDND        = func(model.Presence) bool { return false }
	isPresenting = func(model.Presence) bool { return false }
)
```

They are `var`s rather than plain funcs for one reason: a stub returning a
constant makes its branch in `shouldPush` unreachable, so a plain func would ship
the rule-2 wiring as untested dead code. As vars, tests supply the eventual
behaviour and prove the gate ordering **now**, so when the real predicates land
the only thing left to verify is the predicates themselves.

**Consequence, stated rather than buried: rule 2 of the decision table is
specified and wired but does not fire yet.** Until the presence side ships, a
user in do-not-disturb is governed by whatever `isInCall` already covers.

### `isInCall` is deliberately left alone

`isInCall` keeps matching **both** `busy` and `in-call`, exactly as Spec 2 shipped
it. Narrowing it to `in-call` now — the natural-looking tidy-up once `isDND`
exists — would push notifications to every user in manual do-not-disturb for the
entire gap before the presence side ships. Rule 2 arriving inert is acceptable;
a regression in the meantime is not.

When the real `isDND` lands, `busy` moves out of the `isInCall` bucket in that
same change, and `showNotificationsInCall` narrows to in-call only.

## The gate

```go
func shouldPush(p model.Presence, ns notifSettings, isPrioritySender bool) bool {
	if ns.allowPriority && isPrioritySender {
		return true
	}
	if ns.muteAll {
		return false
	}
	if isDND(p) || isPresenting(p) {
		return false
	}
	if isInCall(p) && !ns.showInCall {
		return false
	}
	return true
}
```

This is the issue's priority order literally: pierce, then mute, then rule 2,
then the in-call bucket. Four decisions are encoded here, each of which could
reasonably have gone the other way. One of them reverses Spec 2.

### The pierce is total, and it is one switch

`alwaysAllowPriorityNotifications` is the single control for "priority contacts
can reach me anyway". It pierces mute, DND and in-call alike.

The issue's table mentions the checkbox only on the mute-all row and leaves the
status rows saying just "Priority contacts", which reads as an unconditional
exemption. That reading is rejected: it would mean merely *adding* someone to
priority contacts silently changes what happens during DND, for a user who never
asked for that. Tying every pierce to one explicit opt-in gives one rule to
document, one setting to find in the UI, and — importantly for rollout — no
behaviour change at all for users who never enabled it.

The early `return true` is deliberate over three `&& !pierce` clauses. It states
"a priority contact reaches you regardless" as one readable fact instead of
something the reader reconstructs from three conditions.

**This reverses Spec 2**, which held that the pierce "deliberately does not cross
the in-call gate", reasoning that letting it through "would make
`showNotificationsInCall` unreachable for exactly the senders most likely to
trigger it". That reasoning was sound but is now overruled by the issue's explicit
"Priority contacts are still exempt". The setting is not unreachable — it governs
every non-priority sender, which is the large majority of traffic.

### `showNotificationsInCall` will not govern DND — but not yet

Rule 2 sits above the in-call check, so once `isDND` is real, a do-not-disturb
user is suppressed whatever `showNotificationsInCall` says. That is the intent:
do-not-disturb means do not disturb, and a checkbox named for calls should not
quietly re-enable pushes during it.

Today it changes nothing, because `isDND` is inert and `busy` is still inside the
`isInCall` bucket. The ordering is what this spec locks in; the moment the
predicate goes live the behaviour follows with no further change to `shouldPush`.

Spec 2 argued the opposite — that splitting `busy` out "would leave `busy` with no
user-facing control at all and no setting to add one". The issue answers that
directly: DND *should* have no control beyond the priority-contact exemption.

### The pierce stays any-room

The issue scopes the mute-all pierce to "Priority contacts' **1-on-1** messages".
Spec 2 shipped it as any-room, and that stands: the setting says *always*, and a
DM-only pierce would be a second undocumented rule the settings UI has no room to
express. Narrowing it now would also silently reduce delivery for users already
relying on the shipped behaviour. Kept unchanged.

### Room rules are absolute

The issue's step 4 — "none of the above → apply each chatroom's individual
notification rules" — invites a short-circuit reading in which a step 1–3
exception means "allow" and room rules are never consulted. Rejected.

Per-room mute (`m.Muted`) and the large-room mention-only throttle run in the
candidate loop, *before* the user-level gate, and no pierce resurrects a member
they dropped. Step 4 is read as "user-level settings never *widen* delivery past a
room rule". This matters precisely because the pierce is any-room: under the other
reading, a priority contact posting in a channel you muted would push you, and
per-room mute would stop being reliable.

Practical consequence: **the candidate loop needs no change at all.** This spec
stays confined to `shouldPush`.

## Behaviour delta

**Exactly one population moves, and it moves toward more delivery, not less.**

| Population | Before | After |
|---|---|---|
| `alwaysAllowPriorityNotifications=true` + sender in `priorityContacts`, presence `busy` or `in-call` | suppressed | **pushed** |

Nothing else changes. `isInCall` is byte-identical to what shipped, and the two
new predicates are inert, so every recipient who is not a pierce case takes the
same path as before. No user loses a notification on this deploy — which is what
makes it safe to ship ahead of the presence side.

Users with no stored settings are unaffected: the zero `notifSettings` has
`allowPriority=false`, so the pierce cannot fire for them. The same holds under
`USER_SETTINGS_ENABLED=false`, whose `noopUserSettings` yields that zero value for
every recipient — the kill switch still restores pre-enforcement behaviour exactly.

**When the presence side ships `isDND`/`isPresenting`,** a second population moves
the other way: users in do-not-disturb who had `showNotificationsInCall` set stop
being pushed. That is a reduction in delivery and belongs in *that* change's
release notes, not this one's.

## Testing

TDD per CLAUDE.md — tests first, confirmed red, then implementation. `shouldPush`
is pure and I/O-free, so the unit table is the whole verification surface; no
integration test is warranted because nothing here touches Mongo, NATS or Valkey.

**`shouldPush` (table-driven, `presence_test.go`).** The matrix of `muteAll` ×
`allowPriority` × `isPrioritySender` × `showInCall` × `dnd` × `presenting` ×
presence status over `{"", "online", "away", "offline", "busy", "in-call"}`. The
`dnd`/`presenting` columns drive a `stubPresenceFlags` helper that swaps the two
vars for the subtest and restores them in `t.Cleanup`, so no row leaks into a
sibling. Named rows for:

- the moved population above;
- the zero `notifSettings` with both stubs inert reproducing pre-change behaviour
  on every status — the rows that pin "no stored settings, no change";
- DND and presenting each suppressing while `showNotificationsInCall` is set,
  pinning that the in-call opt-in does not rescue rule 2;
- a priority sender *without* `allowPriority` still suppressed by DND, presenting
  and mute, pinning that the pierce needs its opt-in;
- `showNotificationsInCall=true` + `in-call` + non-priority sender pushing,
  pinning that the setting is still reachable.

**Stub contract (`presence_test.go`).** `TestDNDAndPresentingStubsAreInert`
asserts both predicates return false for every status we currently receive —
`busy` and `in-call` included. This is the test that fails if someone later
"helpfully" wires DND to `busy`, which is exactly the inference this spec forbids.
`TestIsInCall` is unchanged and still asserts the `busy`+`in-call` bucket.

**Handler wiring (`handler_test.go`).** Two tests using invented statuses
(`stub-dnd`, `stub-presenting`) via `stubPresenceFlagsByStatus`, so they assert
the wiring without asserting a mapping the presence side has yet to define:
one proving both predicates suppress despite `showNotificationsInCall`, one
proving a priority sender pierces that suppression. These complement the
pure-function table by proving the settings and presence maps are actually
combined per-recipient in the survivor loop.

Coverage floor 80%; `shouldPush` and `isInCall` reach 100%.

## Documentation

Two `docs/client-api.md` rows become factually wrong on merge and are corrected in
the same PR:

- `alwaysAllowPriorityNotifications` — drop "The pierce does not override
  `showNotificationsInCall`"; state that the pierce covers mute and every presence
  suppressor, in any room type.
- `showNotificationsInCall` — still governs `"busy"` and `"in-call"`, but the
  priority pierce now *does* bypass it.

Plus the push-filter bullet in §4, which gains the priority exemption.

The docs describe only what the code does **today**. Rule 2 is deliberately
undocumented on the client API surface: writing "do-not-disturb suppresses push"
while `isDND` returns false would document a behaviour clients cannot observe.
That sentence lands in the change that makes the predicate real.

`2026-08-10-notification-settings-enforcement-design.md` gains a superseded-by
pointer at its head so it is not read as current.

## Rollout

Ordinary. No population loses notifications, so there is no silent
reduction-in-delivery to pre-announce and no population to size beforehand.

Release note: priority contacts now reach you while you are busy or in a call
when "always allow priority contact notifications" is enabled;
`USER_SETTINGS_ENABLED=false` reverts to prior behaviour without a rollback.

**The follow-up is the one that needs care.** When the presence side ships DND and
presenting, that change flips `isDND`/`isPresenting` to real implementations *and*
narrows `isInCall` to `in-call` only. It should size
`{"settings.showNotificationsInCall": true, "active": {"$ne": false}}` against
production first, because those users stop receiving pushes while in
do-not-disturb.

**And it must be sequenced consumer-first.** `shouldPush` fails open on
unrecognized presence, and `isInCall` matches the literal strings `busy` and
`in-call`. So if the presence service begins emitting a new representation for
do-not-disturb while any `notification-worker` is still on the old binary, that
worker sees an unknown status, fails open, and pushes to precisely the users the
change exists to protect — a rolling deploy would produce exactly the bug being
fixed, intermittently. Deploy recognition to every `notification-worker` **before**
any producer emits the new representation, and keep emitting the old suppressing
representation (or dual-encode) until the last old worker and every rollback target
is gone.

## Files

| File | Change |
|------|--------|
| `notification-worker/presence.go` | Add inert `isDND`/`isPresenting` stub vars; rewrite `shouldPush`. `isInCall` unchanged. |
| `notification-worker/presence_test.go` | Extend the `shouldPush` table with `dnd`/`presenting`; add the two stub helpers and the inert-contract test. `TestIsInCall` unchanged. |
| `notification-worker/handler.go` | Correct the stale comment above the snapshot calls. |
| `notification-worker/handler_test.go` | Rule-2 suppression and priority pierce, end-to-end, via stubbed statuses. |
| `docs/client-api.md` | Two settings rows; the §4 push-filter bullet. |
| `docs/superpowers/specs/2026-08-10-notification-settings-enforcement-design.md` | Superseded-by pointer. |

## What the presence side still owes this

For whoever picks up the other half, the contract is exactly two functions in
`notification-worker/presence.go`:

```go
func isDND(p model.Presence) bool        // true when the user is in do-not-disturb
func isPresenting(p model.Presence) bool // true when the user is presenting
```

Converting the vars back to plain funcs is fine — `stubPresenceFlags` and
`stubPresenceFlagsByStatus` in `presence_test.go` are the only things that need
the var form, and both are test-only. That change must also drop `"busy"` from
`isInCall`, delete `TestDNDAndPresentingStubsAreInert`, and add the DND sentence
to the `showNotificationsInCall` row in `docs/client-api.md`.
