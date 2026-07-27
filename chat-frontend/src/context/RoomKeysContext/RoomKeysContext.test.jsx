import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, waitFor, act } from '@testing-library/react'
import { RoomKeysProvider, useRoomKeys } from './RoomKeysContext'
import fixture from '../../../test/fixtures/encrypted-message.json'

vi.mock('@/api', () => ({
  subscribeToRoomKeyEvents: vi.fn(),
  requestRoomKey: vi.fn(),
}))

vi.mock('@/context/NatsContext', () => ({
  useNats: () => ({
    user: { account: 'alice', id: 'u1' },
    request: vi.fn(),
    subscribe: vi.fn(),
    publish: vi.fn(),
    requestWithAsyncResult: vi.fn(),
    connected: true,
    error: null,
  }),
}))

import { subscribeToRoomKeyEvents, requestRoomKey } from '@/api'

let lastDecryptHook = null

function Probe() {
  lastDecryptHook = useRoomKeys()
  return null
}

describe('RoomKeysProvider', () => {
  beforeEach(() => {
    lastDecryptHook = null
    vi.mocked(subscribeToRoomKeyEvents).mockReset()
  })

  it('dispatches KEY_RECEIVED for each live event', async () => {
    let savedCb
    vi.mocked(subscribeToRoomKeyEvents).mockImplementation((_n, cb) => {
      savedCb = cb
      return { unsubscribe: vi.fn() }
    })

    render(
      <RoomKeysProvider>
        <Probe />
      </RoomKeysProvider>,
    )

    await waitFor(() => expect(savedCb).toBeDefined())

    act(() => {
      savedCb({
        roomId: 'r2',
        version: 3,
        privateKey: btoa(String.fromCharCode(...new Uint8Array(32).fill(1))),
        timestamp: Date.now(),
      })
    })

    await waitFor(() => expect(lastDecryptHook?.hasKey('r2', 3)).toBe(true))
  })

  it('unsubscribes and clears state on unmount', async () => {
    const unsub = { unsubscribe: vi.fn() }
    vi.mocked(subscribeToRoomKeyEvents).mockReturnValue(unsub)

    const { unmount } = render(
      <RoomKeysProvider>
        <Probe />
      </RoomKeysProvider>,
    )
    unmount()
    expect(unsub.unsubscribe).toHaveBeenCalled()
  })
})

describe('RoomKeysProvider.ensureKey', () => {
  beforeEach(() => {
    lastDecryptHook = null
    vi.mocked(subscribeToRoomKeyEvents).mockReset()
    vi.mocked(requestRoomKey).mockReset()
    // The KEY_RECEIVED path doesn't need live events for these tests,
    // but the provider's effect calls subscribeToRoomKeyEvents — give it
    // a no-op subscription handle so the effect doesn't blow up.
    vi.mocked(subscribeToRoomKeyEvents).mockReturnValue({ unsubscribe: vi.fn() })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('dedupes concurrent calls for the same (roomId, version)', async () => {
    let resolveFetch
    vi.mocked(requestRoomKey).mockImplementationOnce(
      () => new Promise((r) => { resolveFetch = r }),
    )

    render(
      <RoomKeysProvider>
        <Probe />
      </RoomKeysProvider>,
    )
    await waitFor(() => expect(lastDecryptHook).not.toBeNull())

    // Two concurrent callers for the same (roomId, version) share one RPC.
    const p1 = lastDecryptHook.ensureKey('r1', 0, 'site-a')
    const p2 = lastDecryptHook.ensureKey('r1', 0, 'site-a')

    // 32 bytes of 0x01 → base64.
    const privateKeyB64 = btoa(String.fromCharCode(...new Array(32).fill(1)))
    resolveFetch({ roomId: 'r1', version: 0, privateKey: privateKeyB64 })

    await expect(p1).resolves.toBe(true)
    await expect(p2).resolves.toBe(true)
    expect(requestRoomKey).toHaveBeenCalledTimes(1)
    // hasKey reflects reducer state after re-render, not the synchronous ref
    await waitFor(() => expect(lastDecryptHook.hasKey('r1', 0)).toBe(true))
  })

  it('records failure and backs off for 60s, then retries', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })

    vi.mocked(requestRoomKey).mockRejectedValueOnce(new Error('not_room_member'))

    render(
      <RoomKeysProvider>
        <Probe />
      </RoomKeysProvider>,
    )
    await waitFor(() => expect(lastDecryptHook).not.toBeNull())

    await expect(lastDecryptHook.ensureKey('r1', 0, 'site-a')).resolves.toBe(false)
    expect(requestRoomKey).toHaveBeenCalledTimes(1)

    // Within the backoff window: no new RPC.
    await expect(lastDecryptHook.ensureKey('r1', 0, 'site-a')).resolves.toBe(false)
    expect(requestRoomKey).toHaveBeenCalledTimes(1)

    // After 60s, a retry is allowed and the next call succeeds.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(60_001)
    })
    const privateKeyB64 = btoa(String.fromCharCode(...new Array(32).fill(1)))
    vi.mocked(requestRoomKey).mockResolvedValueOnce({
      roomId: 'r1', version: 0, privateKey: privateKeyB64,
    })
    await expect(lastDecryptHook.ensureKey('r1', 0, 'site-a')).resolves.toBe(true)
    expect(requestRoomKey).toHaveBeenCalledTimes(2)
  })

  it('decrypt succeeds immediately after ensureKey resolves, before a re-render', async () => {
    // Regression: the on-demand key fetch must rescue the very message that
    // triggered it. useRoomSubscriptions.decryptAndDispatch awaits ensureKey
    // and then retries decrypt in the SAME async tick — before React has
    // flushed the KEY_RECEIVED dispatch into reducer state. If decrypt can
    // only see keys via reducer state, the retry reads stale state, returns
    // null, and the message falls through to the "[encrypted message]"
    // placeholder even though the key was just fetched successfully.
    vi.mocked(requestRoomKey).mockResolvedValueOnce({
      roomId: 'r1',
      version: fixture.message.version,
      privateKey: fixture.privateKey,
    })

    render(
      <RoomKeysProvider>
        <Probe />
      </RoomKeysProvider>,
    )
    await waitFor(() => expect(lastDecryptHook).not.toBeNull())

    const ok = await lastDecryptHook.ensureKey('r1', fixture.message.version, 'site-a')
    expect(ok).toBe(true)

    // No waitFor / no act flush between ensureKey and decrypt — mirrors the
    // real retry timing in decryptAndDispatch.
    const plaintext = await lastDecryptHook.decrypt({
      roomId: 'r1',
      version: fixture.message.version,
      nonceB64: fixture.message.nonce,
      ciphertextB64: fixture.message.ciphertext,
    })
    expect(plaintext).toBe(fixture.plaintext)
  })

  it('short-circuits to true when the (roomId, version) is already cached', async () => {
    render(
      <RoomKeysProvider>
        <Probe />
      </RoomKeysProvider>,
    )
    await waitFor(() => expect(lastDecryptHook).not.toBeNull())

    // Seed the cache via the existing KEY_RECEIVED path.
    // The subscription cb captured by the provider can be reused; reset the
    // mock to a capturing impl and re-render is overkill — instead, seed by
    // calling ensureKey once with a success mock, then call again and confirm
    // no RPC.
    const privateKeyB64 = btoa(String.fromCharCode(...new Array(32).fill(1)))
    vi.mocked(requestRoomKey).mockResolvedValueOnce({
      roomId: 'r1', version: 0, privateKey: privateKeyB64,
    })
    await expect(lastDecryptHook.ensureKey('r1', 0, 'site-a')).resolves.toBe(true)
    expect(requestRoomKey).toHaveBeenCalledTimes(1)

    await expect(lastDecryptHook.ensureKey('r1', 0, 'site-a')).resolves.toBe(true)
    expect(requestRoomKey).toHaveBeenCalledTimes(1) // no second RPC
  })
})

describe('RoomKeysProvider.seedKeys', () => {
  beforeEach(() => {
    lastDecryptHook = null
    vi.mocked(subscribeToRoomKeyEvents).mockReset()
    vi.mocked(requestRoomKey).mockReset()
    vi.mocked(subscribeToRoomKeyEvents).mockReturnValue({ unsubscribe: vi.fn() })
  })

  it('seeds keys from bootstrap so decrypt works without a live event or fetch', async () => {
    // Regression: on a fresh reload the client holds no keys until a live
    // RoomKeyEvent fires or a message forces an on-demand fetch. subscription.list
    // already carries the current room key, so seeding it up front means the
    // first message in a room decrypts without a placeholder or an extra RPC.
    render(
      <RoomKeysProvider>
        <Probe />
      </RoomKeysProvider>,
    )
    await waitFor(() => expect(lastDecryptHook).not.toBeNull())

    act(() => {
      lastDecryptHook.seedKeys([
        { roomId: 'r1', version: fixture.message.version, privateKey: fixture.privateKey },
      ])
    })

    const plaintext = await lastDecryptHook.decrypt({
      roomId: 'r1',
      version: fixture.message.version,
      nonceB64: fixture.message.nonce,
      ciphertextB64: fixture.message.ciphertext,
    })
    expect(plaintext).toBe(fixture.plaintext)
    expect(requestRoomKey).not.toHaveBeenCalled()
  })

  it('skips entries with invalid base64 without throwing', async () => {
    render(
      <RoomKeysProvider>
        <Probe />
      </RoomKeysProvider>,
    )
    await waitFor(() => expect(lastDecryptHook).not.toBeNull())

    act(() => {
      lastDecryptHook.seedKeys([{ roomId: 'r1', version: 1, privateKey: '!!!not-base64!!!' }])
    })

    expect(lastDecryptHook.hasKey('r1', 1)).toBe(false)
  })

  it('ignores malformed entries (missing roomId / version / privateKey)', async () => {
    render(
      <RoomKeysProvider>
        <Probe />
      </RoomKeysProvider>,
    )
    await waitFor(() => expect(lastDecryptHook).not.toBeNull())

    act(() => {
      lastDecryptHook.seedKeys([
        { roomId: '', version: 1, privateKey: fixture.privateKey },
        { roomId: 'r1', version: undefined, privateKey: fixture.privateKey },
        { roomId: 'r2', version: 1 },
      ])
    })

    expect(lastDecryptHook.hasKey('r1', 1)).toBe(false)
    expect(lastDecryptHook.hasKey('r2', 1)).toBe(false)
  })
})
