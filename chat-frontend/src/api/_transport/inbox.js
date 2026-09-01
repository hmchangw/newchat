// The reply namespace a client subscribes to. Reply subjects live inside the
// client's own user namespace, which the scoped signing-key template already
// grants as `chat.user.{{tag(account)}}.>` — so no separate inbox grant is
// needed and a client can adopt this prefix against an unchanged server.
//
// `account` must be the value auth-service returned in `user.account` — the
// same value every other subject builder already uses. It is used verbatim:
// normalising it here would re-derive server-side logic in the browser, and Go
// and JavaScript disagree on non-ASCII lowercasing, so the prefix would
// silently stop matching the grant for those accounts.
//
// Note the namespace is granted for publish as well as subscribe, so a client
// can publish to its own inbox. That is self-spoofing only — another user's
// publish grant is scoped to their own account — and the bare prefix leaves
// no distinct segment to hang a deny rule on.
export const userInboxPrefix = (account) => `chat.user.${account}`
