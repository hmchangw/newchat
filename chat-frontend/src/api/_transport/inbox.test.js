import { describe, it, expect } from 'vitest'
import { createInbox } from 'nats.ws'
import { userInboxPrefix } from './inbox'

describe('userInboxPrefix', () => {
  it('namespaces the inbox under the account', () => {
    expect(userInboxPrefix('alice')).toBe('chat.user.alice')
  })

  it('uses the account verbatim, without re-normalising it', () => {
    // The account passed in is whatever auth-service returned in
    // `user.account` — the value the `{{tag(account)}}` grant is evaluated
    // against. Folding case here would re-derive Go's ToLower in the browser,
    // and the two disagree on non-ASCII input.
    expect(userInboxPrefix('\u0130User')).toBe('chat.user.\u0130User')
  })

  it('produces a prefix nats.ws expands into a matching subject', () => {
    // Pins nats.ws concatenation: createInbox returns `${prefix}.${nuid}`, and
    // it accepts a multi-token prefix (it only rejects wildcards). A future
    // nats.ws bump that changes either breaks the server-side grant
    // `chat.user.{{tag(account)}}.>`, so it must fail here first.
    const subject = createInbox(userInboxPrefix('alice'))
    expect(subject).toMatch(/^chat\.user\.alice\.[A-Za-z0-9]+$/)
  })
})
