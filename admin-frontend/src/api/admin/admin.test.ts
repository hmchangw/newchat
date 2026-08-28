import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { AsyncJobError } from '@/api'
import {
  createPermissions,
  createUser,
  getUser,
  listAudit,
  listPermissions,
  listRoomMembers,
  listRooms,
  listSessions,
  listUsers,
  resyncPermissions,
  resyncUser,
  revokeAllSessions,
  revokeSession,
  setPassword,
  setRoomOnDuty,
  updateUser,
  uploadClientVersion,
  UPLOAD_TIMEOUT_MS,
  UPLOAD_TIMEOUT_MARGIN_MS,
} from './index'

function mockResponse(status: number, body: unknown): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
  } as unknown as Response
}

function stubFetch(status: number, body: unknown) {
  const fetchMock = vi.fn().mockResolvedValue(mockResponse(status, body))
  vi.stubGlobal('fetch', fetchMock)
  return fetchMock
}

const USER = {
  id: 'u-1',
  account: 'alice',
  siteId: 'site-1',
  engName: 'Alice',
  chineseName: '爱丽丝',
  roles: ['admin'],
  active: true,
  requirePasswordChange: false,
}

const ROOM = {
  id: 'r-1',
  name: 'general',
  type: 'channel',
  userCount: 7,
  restricted: true,
  externalAccess: true,
  onDuty: true,
}

describe('listUsers', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('GETs /v1/admin/users with q/page/limit query params when provided', async () => {
    const fetchMock = stubFetch(200, { users: [USER], total: 1 })

    const result = await listUsers('tok', { q: 'ali', page: 2, limit: 10 })

    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [url, init] = fetchMock.mock.calls[0]
    const parsed = new URL(url)
    expect(parsed.pathname).toBe('/v1/admin/users')
    expect(parsed.searchParams.get('q')).toBe('ali')
    expect(parsed.searchParams.get('page')).toBe('2')
    expect(parsed.searchParams.get('limit')).toBe('10')
    expect(init.method).toBe('GET')
    expect(init.headers.Authorization).toBe('Bearer tok')
    expect(init.headers['Content-Type']).toBe('application/json')
    expect(result).toEqual({ users: [USER], total: 1 })
  })

  it('omits query params entirely when not provided', async () => {
    const fetchMock = stubFetch(200, { users: [], total: 0 })

    await listUsers('tok', {})

    const [url] = fetchMock.mock.calls[0]
    expect(url).toBe('http://localhost:8082/v1/admin/users')
  })

  it('defaults roles/active/requirePasswordChange when the server omits zero-valued fields', async () => {
    stubFetch(200, {
      users: [{ id: 'u-2', account: 'bob', siteId: 'site-1' }],
      total: 1,
    })

    const result = await listUsers('tok', {})

    expect(result.users[0]).toEqual({
      id: 'u-2',
      account: 'bob',
      siteId: 'site-1',
      engName: '',
      chineseName: '',
      roles: [],
      active: true,
      requirePasswordChange: false,
    })
  })

  it('throws AsyncJobError with reason not_admin on a 403', async () => {
    stubFetch(403, { error: 'admin role required', code: 'forbidden', reason: 'not_admin' })

    await expect(listUsers('tok', {})).rejects.toBeInstanceOf(AsyncJobError)
    await expect(listUsers('tok', {})).rejects.toMatchObject({ reason: 'not_admin' })
  })
})

describe('getUser', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('GETs /v1/admin/users/:account and returns the AdminUser', async () => {
    const fetchMock = stubFetch(200, USER)

    const result = await getUser('tok', 'u-1')

    const [url, init] = fetchMock.mock.calls[0]
    expect(url).toBe('http://localhost:8082/v1/admin/users/u-1')
    expect(init.method).toBe('GET')
    expect(init.headers.Authorization).toBe('Bearer tok')
    expect(result).toEqual(USER)
  })

  it('throws AsyncJobError with reason user_not_found on a 404', async () => {
    stubFetch(404, { error: 'user not found', code: 'not_found', reason: 'user_not_found' })

    await expect(getUser('tok', 'missing')).rejects.toBeInstanceOf(AsyncJobError)
    await expect(getUser('tok', 'missing')).rejects.toMatchObject({ reason: 'user_not_found' })
  })
})

describe('createUser', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('POSTs /v1/admin/users with the request body and returns the created AdminUser', async () => {
    const fetchMock = stubFetch(201, USER)

    const result = await createUser('tok', {
      account: 'alice',
      engName: 'Alice',
      chineseName: '爱丽丝',
      roles: ['admin'],
      password: 'hunter2',
      requirePasswordChange: true,
    })

    const [url, init] = fetchMock.mock.calls[0]
    expect(url).toBe('http://localhost:8082/v1/admin/users')
    expect(init.method).toBe('POST')
    expect(init.headers.Authorization).toBe('Bearer tok')
    expect(init.headers['Content-Type']).toBe('application/json')
    expect(JSON.parse(init.body)).toEqual({
      account: 'alice',
      engName: 'Alice',
      chineseName: '爱丽丝',
      roles: ['admin'],
      password: 'hunter2',
      requirePasswordChange: true,
    })
    expect(result.user).toEqual(USER)
  })

  it('createUser defaults absent fanout fields', async () => {
    stubFetch(201, { id: 'u1', account: 'a', siteId: 's', roles: ['bot'], active: true })

    const res = await createUser('tok', { account: 'a', roles: ['bot'], password: 'x' })

    expect(res.user.account).toBe('a')
    expect(res.syncFailures).toEqual([])
    expect(res.hrSyncFailed).toBe(false)
  })

  it('createUser passes through fanout failures', async () => {
    stubFetch(201, {
      id: 'u1',
      account: 'a',
      siteId: 's',
      roles: [],
      active: true,
      syncFailures: ['site-c'],
      hrSyncFailed: true,
    })

    const res = await createUser('tok', { account: 'a', roles: ['bot'], password: 'x' })

    expect(res.syncFailures).toEqual(['site-c'])
    expect(res.hrSyncFailed).toBe(true)
  })

  it('throws AsyncJobError with reason account_exists on a 409', async () => {
    stubFetch(409, {
      error: 'account already exists',
      code: 'conflict',
      reason: 'account_exists',
    })

    await expect(
      createUser('tok', { account: 'dup', roles: [], password: 'x' }),
    ).rejects.toBeInstanceOf(AsyncJobError)
    await expect(
      createUser('tok', { account: 'dup', roles: [], password: 'x' }),
    ).rejects.toMatchObject({ reason: 'account_exists' })
  })
})

describe('updateUser', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('PATCHes /v1/admin/users/:account with the partial patch body', async () => {
    const fetchMock = stubFetch(200, { status: 'ok' })

    await updateUser('tok', 'u-1', { active: false })

    const [url, init] = fetchMock.mock.calls[0]
    expect(url).toBe('http://localhost:8082/v1/admin/users/u-1')
    expect(init.method).toBe('PATCH')
    expect(init.headers.Authorization).toBe('Bearer tok')
    expect(JSON.parse(init.body)).toEqual({ active: false })
  })

  it('updateUser returns syncFailures', async () => {
    stubFetch(200, { status: 'ok', syncFailures: ['site-b'] })

    const res = await updateUser('tok', 'a', { roles: ['admin'] })

    expect(res.syncFailures).toEqual(['site-b'])
  })
})

describe('setPassword', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('POSTs /v1/admin/users/:account/password, mapping newPassword to the wire "password" field', async () => {
    const fetchMock = stubFetch(200, { status: 'ok' })

    await setPassword('tok', 'u-1', { newPassword: 'newpw', requirePasswordChange: true })

    const [url, init] = fetchMock.mock.calls[0]
    expect(url).toBe('http://localhost:8082/v1/admin/users/u-1/password')
    expect(init.method).toBe('POST')
    expect(JSON.parse(init.body)).toEqual({ password: 'newpw', requirePasswordChange: true })
  })

  it('omits requirePasswordChange from the body when not provided', async () => {
    const fetchMock = stubFetch(200, { status: 'ok' })

    await setPassword('tok', 'u-1', { newPassword: 'newpw' })

    const [, init] = fetchMock.mock.calls[0]
    expect(JSON.parse(init.body)).toEqual({ password: 'newpw' })
  })
})

describe('listSessions', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('GETs /v1/admin/sessions?account=:account and unwraps the {sessions:[]} envelope to an array', async () => {
    const sessions = [{ id: 's-1', userId: 'u-1', account: 'alice', siteId: 'site-1', issuedAt: 1234 }]
    const fetchMock = stubFetch(200, { sessions })

    const result = await listSessions('tok', 'alice')

    const [url, init] = fetchMock.mock.calls[0]
    expect(url).toBe('http://localhost:8082/v1/admin/sessions?account=alice')
    expect(init.method).toBe('GET')
    expect(result).toEqual(sessions)
  })
})

describe('revokeAllSessions', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('DELETEs /v1/admin/sessions?account=:account', async () => {
    const fetchMock = stubFetch(200, { status: 'ok' })

    await revokeAllSessions('tok', 'alice')

    const [url, init] = fetchMock.mock.calls[0]
    expect(url).toBe('http://localhost:8082/v1/admin/sessions?account=alice')
    expect(init.method).toBe('DELETE')
    expect(init.headers.Authorization).toBe('Bearer tok')
  })
})

describe('revokeSession', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('DELETEs /v1/admin/sessions/:sessionId?account=:account', async () => {
    const fetchMock = stubFetch(200, { status: 'ok' })

    await revokeSession('tok', 'alice', 's-1')

    const [url, init] = fetchMock.mock.calls[0]
    expect(url).toBe('http://localhost:8082/v1/admin/sessions/s-1?account=alice')
    expect(init.method).toBe('DELETE')
  })
})

describe('listAudit', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('GETs /v1/admin/audit with filter query params when provided', async () => {
    const entries = [
      {
        id: 'a-1',
        actorUserId: 'u-9',
        actorAccount: 'root',
        action: 'user.create',
        targetUserId: 'u-1',
        siteId: 'site-1',
        timestamp: 1000,
      },
    ]
    const fetchMock = stubFetch(200, { entries, total: 1 })

    const result = await listAudit('tok', {
      targetAccount: 'alice',
      actor: 'root',
      action: 'user.create',
      page: 1,
      limit: 20,
    })

    const [url, init] = fetchMock.mock.calls[0]
    const parsed = new URL(url)
    expect(parsed.pathname).toBe('/v1/admin/audit')
    expect(parsed.searchParams.get('targetAccount')).toBe('alice')
    expect(parsed.searchParams.get('actor')).toBe('root')
    expect(parsed.searchParams.get('action')).toBe('user.create')
    expect(parsed.searchParams.get('page')).toBe('1')
    expect(parsed.searchParams.get('limit')).toBe('20')
    expect(init.method).toBe('GET')
    expect(result).toEqual({ entries, total: 1 })
  })

  it('omits filter query params entirely when not provided', async () => {
    const fetchMock = stubFetch(200, { entries: [], total: 0 })

    await listAudit('tok', {})

    const [url] = fetchMock.mock.calls[0]
    expect(url).toBe('http://localhost:8082/v1/admin/audit')
  })
})

const GRANT_VIEW = {
  id: 'g-1',
  permission: 'external.image.view',
  subjectAccount: 'alice',
  granted: true,
  effectiveFrom: '2026-09-01',
  expiresAt: '2026-12-31',
  applicantAccount: 'carol',
  approverAccount: 'dave',
  reason: 'On-call staff must review production line photos from outside the fab.',
  recordedBy: 'p_admin_wang',
  recordedAt: '2026-08-11T03:00:00Z',
}

describe('createPermissions', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('POSTs /v1/admin/permissions with the request body and returns the created response', async () => {
    const response = {
      created: 2,
      duplicatesIgnored: [],
    }
    const fetchMock = stubFetch(201, response)

    const result = await createPermissions('tok', {
      permission: 'external.image.view',
      subjectAccounts: ['alice', 'bob'],
      granted: true,
      effectiveFrom: '2026-09-01',
      expiresAt: '2026-12-31',
      applicantAccount: 'carol',
      approverAccount: 'dave',
      reason: 'On-call staff must review production line photos from outside the fab.',
    })

    const [url, init] = fetchMock.mock.calls[0]
    expect(url).toBe('http://localhost:8082/v1/admin/permissions')
    expect(init.method).toBe('POST')
    expect(init.headers.Authorization).toBe('Bearer tok')
    expect(init.headers['Content-Type']).toBe('application/json')
    expect(JSON.parse(init.body)).toEqual({
      permission: 'external.image.view',
      subjectAccounts: ['alice', 'bob'],
      granted: true,
      effectiveFrom: '2026-09-01',
      expiresAt: '2026-12-31',
      applicantAccount: 'carol',
      approverAccount: 'dave',
      reason: 'On-call staff must review production line photos from outside the fab.',
    })
    expect(result).toEqual(response)
  })

  it('throws AsyncJobError with reason preserved on a non-2xx response', async () => {
    stubFetch(404, {
      error: 'unknown accounts: zzz',
      code: 'not_found',
      reason: 'unknown_accounts',
    })
    const body = {
      permission: 'external.image.view',
      subjectAccounts: ['zzz'],
      granted: true,
      expiresAt: '2026-12-31',
      applicantAccount: 'carol',
      approverAccount: 'dave',
      reason: 'test',
    }

    await expect(createPermissions('tok', body)).rejects.toBeInstanceOf(AsyncJobError)
    await expect(createPermissions('tok', body)).rejects.toMatchObject({
      reason: 'unknown_accounts',
    })
  })

  it('returns syncFailures when the response carries them', async () => {
    stubFetch(201, {
      created: 2,
      duplicatesIgnored: [],
      syncFailures: ['site-2'],
    })

    const result = await createPermissions('tok', {
      permission: 'external.image.view',
      subjectAccounts: ['alice', 'bob'],
      granted: true,
      expiresAt: '2026-12-31',
      applicantAccount: 'carol',
      approverAccount: 'dave',
      reason: 'test',
    })

    expect(result.syncFailures).toEqual(['site-2'])
  })
})

describe('resyncPermissions', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('POSTs /v1/admin/permissions/resync with the request body and returns the response', async () => {
    const fetchMock = stubFetch(200, { syncFailures: ['site-2'] })

    const result = await resyncPermissions('tok', {
      permission: 'external.image.view',
      accounts: ['alice', 'bob'],
    })

    const [url, init] = fetchMock.mock.calls[0]
    expect(url).toBe('http://localhost:8082/v1/admin/permissions/resync')
    expect(init.method).toBe('POST')
    expect(init.headers.Authorization).toBe('Bearer tok')
    expect(init.headers['Content-Type']).toBe('application/json')
    expect(JSON.parse(init.body)).toEqual({
      permission: 'external.image.view',
      accounts: ['alice', 'bob'],
    })
    expect(result).toEqual({ syncFailures: ['site-2'] })
  })

  it('resolves with an empty object when the server reports no sync failures', async () => {
    stubFetch(200, {})

    const result = await resyncPermissions('tok', {
      permission: 'external.image.view',
      accounts: ['alice'],
    })

    expect(result.syncFailures).toBeUndefined()
  })

  it('throws AsyncJobError with reason preserved on a non-2xx response', async () => {
    stubFetch(404, {
      error: 'unknown accounts: zzz',
      code: 'not_found',
      reason: 'unknown_accounts',
    })

    await expect(
      resyncPermissions('tok', { permission: 'external.image.view', accounts: ['zzz'] }),
    ).rejects.toMatchObject({ reason: 'unknown_accounts' })
  })
})

describe('listPermissions', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('GETs /v1/admin/permissions with subjectAccount plus optional params when all are provided', async () => {
    const fetchMock = stubFetch(200, { entries: [GRANT_VIEW], total: 1, currentlyGranted: true })

    const result = await listPermissions('tok', {
      subjectAccount: 'alice',
      permission: 'external.image.view',
      page: 2,
      limit: 10,
    })

    const [url, init] = fetchMock.mock.calls[0]
    const parsed = new URL(url)
    expect(parsed.pathname).toBe('/v1/admin/permissions')
    expect(parsed.searchParams.get('subjectAccount')).toBe('alice')
    expect(parsed.searchParams.get('permission')).toBe('external.image.view')
    expect(parsed.searchParams.get('page')).toBe('2')
    expect(parsed.searchParams.get('limit')).toBe('10')
    expect(init.method).toBe('GET')
    expect(result).toEqual({ entries: [GRANT_VIEW], total: 1, currentlyGranted: true })
  })

  it('omits permission/page/limit from the query string when not provided', async () => {
    const fetchMock = stubFetch(200, { entries: [], total: 0 })

    await listPermissions('tok', { subjectAccount: 'alice' })

    const [url] = fetchMock.mock.calls[0]
    expect(url).toBe('http://localhost:8082/v1/admin/permissions?subjectAccount=alice')
  })

  it('omits subjectAccount from the query string when not provided (unfiltered list)', async () => {
    const fetchMock = stubFetch(200, { entries: [], total: 0 })

    await listPermissions('tok', {})

    const [url] = fetchMock.mock.calls[0]
    expect(url).toBe('http://localhost:8082/v1/admin/permissions')
  })

  it('GETs with permission only when subjectAccount is omitted', async () => {
    const fetchMock = stubFetch(200, { entries: [], total: 0 })

    await listPermissions('tok', { permission: 'external.image.view', page: 1, limit: 20 })

    const [url] = fetchMock.mock.calls[0]
    const parsed = new URL(url)
    expect(parsed.searchParams.get('subjectAccount')).toBeNull()
    expect(parsed.searchParams.get('permission')).toBe('external.image.view')
    expect(parsed.searchParams.get('page')).toBe('1')
    expect(parsed.searchParams.get('limit')).toBe('20')
  })
})

describe('resyncUser', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('POSTs /v1/admin/users/:account/resync with no body', async () => {
    const fetchMock = stubFetch(200, { status: 'ok' })

    const res = await resyncUser('tok', 'u-1')

    const [url, init] = fetchMock.mock.calls[0]
    expect(url).toBe('http://localhost:8082/v1/admin/users/u-1/resync')
    expect(init.method).toBe('POST')
    expect(init.body).toBeUndefined()
    expect(init.headers.Authorization).toBe('Bearer tok')
    expect(res).toEqual({ syncFailures: [], hrSyncFailed: false })
  })

  it('maps syncFailures and hrSyncFailed when present', async () => {
    stubFetch(200, { status: 'ok', syncFailures: ['site-b'], hrSyncFailed: true })

    const res = await resyncUser('tok', 'a')

    expect(res).toEqual({ syncFailures: ['site-b'], hrSyncFailed: true })
  })
})

describe('uploadClientVersion', () => {
  class MockXHR {
    static instances: MockXHR[] = []
    upload = { onprogress: null as ((e: ProgressEvent) => void) | null }
    onload: (() => void) | null = null
    onerror: (() => void) | null = null
    onabort: (() => void) | null = null
    ontimeout: (() => void) | null = null
    onloadend: (() => void) | null = null
    timeout = 0
    aborted = false
    status = 0
    responseText = ''
    method = ''
    url = ''
    headers: Record<string, string> = {}
    body: FormData | null = null

    constructor() {
      MockXHR.instances.push(this)
    }
    open(method: string, url: string) {
      this.method = method
      this.url = url
    }
    setRequestHeader(k: string, v: string) {
      this.headers[k] = v
    }
    send(body: FormData) {
      this.body = body
    }
    abort() {
      this.aborted = true
      this.onabort?.()
      this.onloadend?.()
    }
    // Test helper: settle the request. Success and failure differ only in the
    // status and body the server would have sent, so one method covers both.
    respond(status = 200, text = '{"result":"success"}') {
      this.status = status
      this.responseText = text
      this.onload?.()
      this.onloadend?.()
    }
  }

  beforeEach(() => {
    MockXHR.instances = []
    vi.stubGlobal('XMLHttpRequest', MockXHR)
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  const cfgFile = () => new File(['version: 1'], 'app.yaml', { type: 'text/yaml' })
  const exeFile = () => new File(['MZ'], 'app.exe', { type: 'application/octet-stream' })

  it('posts both files as multipart with the bearer token', async () => {
    const promise = uploadClientVersion('tok-123', cfgFile(), exeFile())
    const xhr = MockXHR.instances[0]
    xhr.respond()
    await promise

    expect(xhr.method).toBe('POST')
    expect(xhr.url).toContain('/v1/admin/client-updates')
    expect(xhr.headers.Authorization).toBe('Bearer tok-123')
    expect(xhr.body?.get('configFile')).toBeInstanceOf(File)
    expect(xhr.body?.get('executeFile')).toBeInstanceOf(File)
  })

  // The browser must write its own multipart boundary; setting Content-Type by
  // hand produces a body the server cannot parse.
  it('never sets Content-Type by hand', async () => {
    const promise = uploadClientVersion('tok-123', cfgFile(), exeFile())
    const xhr = MockXHR.instances[0]
    xhr.respond()
    await promise

    const keys = Object.keys(xhr.headers).map((k) => k.toLowerCase())
    expect(keys).not.toContain('content-type')
  })

  it('reports upload progress as a percentage', async () => {
    const seen: number[] = []
    const promise = uploadClientVersion('tok-123', cfgFile(), exeFile(), (pct) => seen.push(pct))
    const xhr = MockXHR.instances[0]
    xhr.upload.onprogress?.({ lengthComputable: true, loaded: 50, total: 200 } as ProgressEvent)
    xhr.upload.onprogress?.({ lengthComputable: true, loaded: 200, total: 200 } as ProgressEvent)
    xhr.respond()
    await promise

    expect(seen).toEqual([25, 100])
  })

  it('ignores progress events whose total is unknown', async () => {
    const seen: number[] = []
    const promise = uploadClientVersion('tok-123', cfgFile(), exeFile(), (pct) => seen.push(pct))
    const xhr = MockXHR.instances[0]
    xhr.upload.onprogress?.({ lengthComputable: false, loaded: 50, total: 0 } as ProgressEvent)
    xhr.respond()
    await promise

    expect(seen).toEqual([])
  })

  it('throws AsyncJobError carrying the envelope on a non-2xx response', async () => {
    const promise = uploadClientVersion('tok-123', cfgFile(), exeFile())
    const xhr = MockXHR.instances[0]
    xhr.respond(400, '{"code":"bad_request","error":"configFile must be a .yaml or .yml file"}')

    await expect(promise).rejects.toMatchObject({
      name: 'AsyncJobError',
      code: 'bad_request',
      message: 'configFile must be a .yaml or .yml file',
    })
  })

  it('throws AsyncJobError when the response body is not JSON', async () => {
    const promise = uploadClientVersion('tok-123', cfgFile(), exeFile())
    MockXHR.instances[0].respond(502, '<html>gateway</html>')
    await expect(promise).rejects.toBeInstanceOf(Error)
  })

  it('rejects on a transport error', async () => {
    const promise = uploadClientVersion('tok-123', cfgFile(), exeFile())
    MockXHR.instances[0].onerror?.()
    await expect(promise).rejects.toBeInstanceOf(Error)
  })

  // Without an onabort handler the promise never settles and the console's
  // submit button stays disabled forever.
  it('rejects on abort', async () => {
    const promise = uploadClientVersion('tok-123', cfgFile(), exeFile())
    MockXHR.instances[0].onabort?.()
    await expect(promise).rejects.toMatchObject({ name: 'AsyncJobError' })
  })

  // Without an ontimeout handler the promise never settles and the console's
  // submit button stays disabled forever.
  it('rejects on timeout', async () => {
    const promise = uploadClientVersion('tok-123', cfgFile(), exeFile())
    MockXHR.instances[0].ontimeout?.()
    await expect(promise).rejects.toMatchObject({ name: 'AsyncJobError' })
  })

  // xhr.timeout defaults to 0, which disables timeouts entirely and makes the
  // ontimeout handler above unreachable. Asserting the configured value is what
  // proves a stalled upload can actually settle.
  it('configures a non-zero upload timeout so ontimeout can fire', async () => {
    const promise = uploadClientVersion('tok-123', cfgFile(), exeFile())
    const xhr = MockXHR.instances[0]

    expect(xhr.timeout).toBe(UPLOAD_TIMEOUT_MS)
    expect(xhr.timeout).toBeGreaterThan(0)

    xhr.respond()
    await promise
  })
})

describe('uploadClientVersion cancellation and budget', () => {
  class AbortableXHR {
    static instances: AbortableXHR[] = []
    upload = { onprogress: null as ((e: ProgressEvent) => void) | null }
    onload: (() => void) | null = null
    onerror: (() => void) | null = null
    onabort: (() => void) | null = null
    ontimeout: (() => void) | null = null
    onloadend: (() => void) | null = null
    status = 0
    responseText = ''
    timeout = 0
    aborted = false
    sent = false
    constructor() {
      AbortableXHR.instances.push(this)
    }
    open() {}
    setRequestHeader() {}
    send() {
      this.sent = true
    }
    // Per the XHR spec, abort() fires abort/loadend only for a request that was
    // actually sent. A mock that fires them unconditionally hides a caller that
    // relies on the event to settle its promise before send().
    abort() {
      this.aborted = true
      if (!this.sent) return
      this.onabort?.()
      this.onloadend?.()
    }
    respond(status = 200, text = '{"result":"success"}') {
      this.status = status
      this.responseText = text
      this.onload?.()
      this.onloadend?.()
    }
  }

  beforeEach(() => {
    AbortableXHR.instances = []
    vi.stubGlobal('XMLHttpRequest', AbortableXHR)
  })
  afterEach(() => vi.unstubAllGlobals())

  const cfg = () => new File(['version: 1'], 'app.yaml', { type: 'text/yaml' })
  const exe = () => new File(['MZ'], 'app.exe', { type: 'application/octet-stream' })

  // The XHR timer starts before the request reaches admin-service, so an equal
  // budget lets the browser abort an upload the backend already committed.
  it('allows a longer budget than the backend relay timeout', () => {
    expect(UPLOAD_TIMEOUT_MARGIN_MS).toBeGreaterThan(0)
    expect(UPLOAD_TIMEOUT_MS).toBeGreaterThan(10 * 60 * 1000)
  })

  it('aborts the request when the signal fires', async () => {
    const controller = new AbortController()
    const promise = uploadClientVersion('tok', cfg(), exe(), undefined, controller.signal)

    controller.abort()

    expect(AbortableXHR.instances[0].aborted).toBe(true)
    await expect(promise).rejects.toMatchObject({ name: 'AsyncJobError' })
  })

  it('does not send when the signal is already aborted', async () => {
    const controller = new AbortController()
    controller.abort()
    const promise = uploadClientVersion('tok', cfg(), exe(), undefined, controller.signal)

    expect(AbortableXHR.instances[0].aborted).toBe(true)
    // A request that was never sent fires no abort event, so the promise has to
    // be settled by the caller — otherwise the console stays busy forever.
    expect(AbortableXHR.instances[0].sent).toBe(false)
    await expect(promise).rejects.toMatchObject({ name: 'AsyncJobError' })
  })

  // A long-lived controller must not retain the listener (and the xhr it closes
  // over) after the request settles.
  it('detaches the abort listener once the request settles', async () => {
    const controller = new AbortController()
    const removeSpy = vi.spyOn(controller.signal, 'removeEventListener')
    const promise = uploadClientVersion('tok', cfg(), exe(), undefined, controller.signal)

    AbortableXHR.instances[0].respond()
    await promise

    expect(removeSpy).toHaveBeenCalledWith('abort', expect.any(Function))
  })
})

describe('listRooms', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('GETs /v1/admin/rooms with page/limit query params', async () => {
    const fetchMock = stubFetch(200, { rooms: [ROOM], total: 1 })

    const result = await listRooms('tok', { page: 2, limit: 10 })

    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [url, init] = fetchMock.mock.calls[0]
    const parsed = new URL(url)
    expect(parsed.pathname).toBe('/v1/admin/rooms')
    expect(parsed.searchParams.get('page')).toBe('2')
    expect(parsed.searchParams.get('limit')).toBe('10')
    expect(init.method).toBe('GET')
    expect(init.headers.Authorization).toBe('Bearer tok')
    expect(result).toEqual({ rooms: [ROOM], total: 1 })
  })

  it('throws AsyncJobError on a non-2xx response', async () => {
    stubFetch(403, { error: { code: 'forbidden', reason: 'not_admin', message: 'nope' } })

    await expect(listRooms('tok')).rejects.toBeInstanceOf(AsyncJobError)
  })
})

describe('listRoomMembers', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('GETs /v1/admin/rooms/<id>/members and returns the members array', async () => {
    const fetchMock = stubFetch(200, {
      members: [
        { account: 'alice', isBot: false },
        { account: 'helperbot', isBot: true },
      ],
    })

    const result = await listRoomMembers('tok', 'room 1')

    const [url, init] = fetchMock.mock.calls[0]
    // The room id is path-encoded, so an id with a space cannot break the URL.
    expect(new URL(url).pathname).toBe('/v1/admin/rooms/room%201/members')
    expect(init.method).toBe('GET')
    expect(result).toEqual([
      { account: 'alice', isBot: false },
      { account: 'helperbot', isBot: true },
    ])
  })

  it('returns an empty array when the server omits members', async () => {
    stubFetch(200, {})

    await expect(listRoomMembers('tok', 'r-1')).resolves.toEqual([])
  })
})

describe('setRoomOnDuty', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('POSTs onDuty:true with the owner account', async () => {
    const fetchMock = stubFetch(200, { status: 'ok' })

    await setRoomOnDuty('tok', 'r-1', { onDuty: true, ownerAccount: 'alice' })

    const [url, init] = fetchMock.mock.calls[0]
    expect(new URL(url).pathname).toBe('/v1/admin/rooms/r-1/onduty')
    expect(init.method).toBe('POST')
    expect(JSON.parse(init.body)).toEqual({ onDuty: true, ownerAccount: 'alice' })
  })

  it('POSTs onDuty:false without an owner account', async () => {
    const fetchMock = stubFetch(200, { status: 'ok' })

    await setRoomOnDuty('tok', 'r-1', { onDuty: false })

    const [, init] = fetchMock.mock.calls[0]
    expect(JSON.parse(init.body)).toEqual({ onDuty: false })
  })

  it('throws AsyncJobError when the room has too few members', async () => {
    stubFetch(409, { error: { code: 'conflict', message: 'not enough members' } })

    await expect(setRoomOnDuty('tok', 'r-1', { onDuty: true, ownerAccount: 'alice' })).rejects.toBeInstanceOf(
      AsyncJobError,
    )
  })
})
