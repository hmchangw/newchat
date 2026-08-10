# Priority-Contact Presence Gating (Spec 3 of 3)

Issue #221. Spec 1 (`2026-08-08-priority-contacts-storage-api-design.md`) stored the
settings. Spec 2 (`2026-08-10-notification-settings-enforcement-design.md`) made
`notification-worker` enforce three of them. This spec finishes the decision table:
presence-based suppression gains a priority-contact exemption, and manual
do-not-disturb stops borrowing the in-call checkbox.

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

## Mapping the issue onto our status vocabulary

The issue names three presence conditions — `Do not disturb`, `presenting`, and
`In a call`. Our vocabulary has two relevant values, and they do not line up
one-for-one:

| Issue term | Our value | Where it comes from |
|---|---|---|
| `Do not disturb` | `busy` | Manual override (`settings`/`presence.status`), `model.StatusBusy` |
| `presenting` | `in-call` | Teams activity `Presenting`, folded into in-call by `user-presence-service/sync/reconcile.go` |
| `In a call` | `in-call` | Teams activities `InACall` / `InAConferenceCall` |

**Decision: keep `Presenting` folded into `in-call`; split `busy` out on its own.**

Making `presenting` a literal third status would mean a new `model.PresenceStatus`
value, a change to `reconcile.go`'s `callActivities` mapping, a new value in the
`docs/client-api.md` presence enum, and every client that switches on presence
needing an update — all to distinguish two states the issue then treats
identically except for which checkbox governs them. `Presenting` is a call
activity; it is in-call in every sense our system models.

The consequence is stated plainly rather than buried: **a Teams user who is
Presenting is treated as in-call, so `showNotificationsInCall` can let those
pushes through.** Under a literal reading of the issue, Presenting would suppress
unconditionally. If that distinction turns out to matter to users, it is a
presence-service change, not a notification-worker one.

`busy`, by contrast, is already a distinct stored value and needs no new plumbing
to separate.

## The gate

```go
// isDND reports manual do-not-disturb. Separate from isInCall because
// showNotificationsInCall governs only the latter.
func isDND(p model.Presence) bool { return p.AggregatedStatus == string(model.StatusBusy) }

// isInCall reports an active call. Teams "Presenting" arrives here too — see the
// status-vocabulary mapping in the spec.
func isInCall(p model.Presence) bool { return p.AggregatedStatus == string(model.StatusInCall) }

// shouldPush applies the priority-contact pierce, then three independent
// suppressors in the issue's stated priority order.
func shouldPush(p model.Presence, ns notifSettings, isPrioritySender bool) bool {
	if ns.allowPriority && isPrioritySender {
		return true
	}
	if ns.muteAll {
		return false
	}
	if isDND(p) {
		return false
	}
	if isInCall(p) && !ns.showInCall {
		return false
	}
	return true
}
```

Four decisions are encoded here, each of which could reasonably have gone the
other way. Two of them reverse Spec 2.

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

### `showNotificationsInCall` no longer governs DND

Spec 2 bucketed `busy` and `in-call` together under one predicate, arguing that
splitting them "would leave `busy` with no user-facing control at all and no
setting to add one".

The issue answers that objection directly: DND *should* have no user-facing
control beyond the priority-contact exemption. Do-not-disturb means do not
disturb — a checkbox named for calls should not quietly re-enable pushes during
it. `busy` is also the one status the user sets by hand, so an explicit intent
already exists and does not need a second one layered on top.

**This reverses Spec 2**, which is left in place as the historical record with a
superseded-by pointer at its head.

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

Exactly two populations move. Everyone else is bit-for-bit unchanged.

| Population | Before | After |
|---|---|---|
| `showNotificationsInCall=true`, presence `busy` | pushed | **suppressed** |
| `alwaysAllowPriorityNotifications=true` + sender in `priorityContacts`, presence `busy` or `in-call` | suppressed | **pushed** |

Users with no stored settings are unaffected: the zero `notifSettings` has
`showInCall=false` and `allowPriority=false`, so `busy` and `in-call` both
suppressed before this change and both suppress after it. The same holds under
`USER_SETTINGS_ENABLED=false`, whose `noopUserSettings` yields that zero value for
every recipient — so the kill switch still restores pre-enforcement behaviour
exactly.

## Testing

TDD per CLAUDE.md — tests first, confirmed red, then implementation. `shouldPush`
is pure and I/O-free, so the unit table is the whole verification surface; no
integration test is warranted because nothing here touches Mongo, NATS or Valkey.

**`shouldPush` (table-driven, `presence_test.go`).** The matrix of `muteAll` ×
`allowPriority` × `isPrioritySender` × `showInCall` × presence status over
`{"", "online", "away", "offline", "busy", "in-call"}`. Named rows for:

- the two moved populations above, one row each, named for the change;
- the zero `notifSettings` reproducing pre-change behaviour on every status —
  this is the row that pins "no stored settings, no change";
- a priority sender *without* `allowPriority` still suppressed by DND and by
  mute, pinning that the pierce needs its opt-in;
- `showNotificationsInCall=true` + `in-call` + non-priority sender pushing,
  pinning that the setting is still reachable.

**`isDND` / `isInCall` (`presence_test.go`).** `TestIsInCall` splits into
`TestIsDND` and `TestIsInCall`, each asserting the *other* status is false —
that pair is what fails if anyone re-merges the bucket.

**Pierce end-to-end (`handler_test.go`).** A `busy` recipient with
`alwaysAllowPriorityNotifications` and the sender in `priorityContacts` is emitted
to. Complements the pure-function table by proving the settings and presence maps
are actually combined per-recipient in the survivor loop.

Coverage floor 80%; `shouldPush` and its predicates are core logic and should
reach 100% given how small they are.

## Documentation

Three `docs/client-api.md` rows become factually wrong on merge and are corrected
in the same PR:

- `alwaysAllowPriorityNotifications` — drop "The pierce does not override
  `showNotificationsInCall`"; state that the pierce covers mute, DND and in-call,
  in any room type.
- `showNotificationsInCall` — it governs `"in-call"` only, no longer `"busy"`; the
  priority pierce *does* bypass it.
- `muteAllNotifications` — unchanged in substance; verify the cross-reference
  still reads correctly beside the two rewritten rows.

Plus the push-filter bullet in §4 ("Presence-busy / in-call recipients are not
pushed; everyone else…"), which now needs the DND/in-call split and the priority
exemption.

`2026-08-10-notification-settings-enforcement-design.md` gains a one-line
superseded-by pointer at its head so it is not read as current.

## Rollout

Population 1 is the one that matters: users who explicitly enabled
`showNotificationsInCall` and use manual DND stop receiving pushes while
do-not-disturb is set. That is the intended outcome and it is a silent,
user-visible reduction in delivery, so it belongs in the release notes rather than
arriving as a report of missing notifications.

Release note: do-not-disturb now suppresses push regardless of the "show
notifications in call" setting; priority contacts pierce do-not-disturb and
in-call when "always allow priority contact notifications" is enabled;
`USER_SETTINGS_ENABLED=false` reverts to prior behaviour without a rollback.

Worth sizing population 1 against production `users` before deploying —
`{"settings.showNotificationsInCall": true, "active": {"$ne": false}}` — so the
change lands as a known number.

## Files

| File | Change |
|------|--------|
| `notification-worker/presence.go` | Split `isInCall` into `isDND` + `isInCall`; rewrite `shouldPush`. |
| `notification-worker/presence_test.go` | Extend the `shouldPush` table; split `TestIsInCall`. |
| `notification-worker/handler.go` | Correct the stale comment above the snapshot calls. |
| `notification-worker/handler_test.go` | End-to-end priority pierce past a `busy` recipient. |
| `docs/client-api.md` | Two settings rows; the §4 push-filter bullet. |
| `docs/superpowers/specs/2026-08-10-notification-settings-enforcement-design.md` | Superseded-by pointer. |
