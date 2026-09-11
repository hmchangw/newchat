# client-update-service — resolving the four open review threads from PR #187

**Date:** 2026-09-11
**Services:** `client-update-service`
**Branch:** `claude/exciting-clarke-16sy3o`

## Goal

Resolve the four review threads left unaddressed when
[#187](https://github.com/hmchangw/newchat/pull/187) merged. All four are still
open on that PR and still live in `main`; the follow-up that shipped since
([#386](https://github.com/hmchangw/newchat/pull/386)) closed the *auth* gap the
design doc deferred and touched only the upload path, so none of these moved.

| # | Thread | Raised by | Severity |
|---|---|---|---|
| 1 | [Bucket creation must go](https://github.com/hmchangw/newchat/pull/187#discussion_r3758893963) — "We can't create bucket using managed minio service" | mliu33 | — |
| 2 | [UTF-8 filename in `Content-Disposition`](https://github.com/hmchangw/newchat/pull/187#discussion_r3759055380) | mliu33 | — |
| 3 | [Distinct cache misses can allocate unbounded memory](https://github.com/hmchangw/newchat/pull/187#discussion_r3755958688) | julianshen, seconded by mliu33 | 🔴 Blocking |
| 4 | [One disconnected client can cancel every coalesced download](https://github.com/hmchangw/newchat/pull/187#discussion_r3755958698) | julianshen, deferred to a later PR by mliu33 | 🟠 Warning |

Items 3 and 4 are both on the download path and both concern `blobCache`; 1 and 2
are independent one-file changes.

## Non-goals

- **Artifact-pair atomicity.** The two objects are still written independently,
  and two concurrent uploads can still interleave. Declined with reasoning on
  #386; the real fix is immutable release staging plus an atomic pointer switch,
  which changes the storage layout and the download contract.
- **Auth on `GET /api/v1/version/:fileName`.** Deliberately unauthenticated — the
  deployed client fleet holds no credential.
- **Sharing `contentDisposition` with `upload-service`.** Four duplicated lines
  beat cross-service refactoring in a review-fix PR.
- **Upload-path changes of any kind.** #386 owns that surface.

## Resolved decisions

1. **Bucket: probe, don't create.** Drop `MakeBucket`; keep a read-only
   `BucketExists` check that fails startup when the bucket is missing. Chosen
   over deleting the check entirely (loses fail-fast on a misconfigured
   `MINIO_BUCKET`) and over a `BOOTSTRAP_BUCKET` opt-in flag mirroring
   `BOOTSTRAP_STREAMS` (more machinery than the thread asked for). Matches
   `pkg/minioutil.NewBucket`, documented as *"fails fast if the bucket does not
   exist. Does not create it."*
2. **Degrade to streaming, never reject.** The answer to cache-fill pressure is
   an uncached stream, not a `429`. The client cannot tell the difference.
3. **The fill budget counts bytes, not requests.** A semaphore bounds how many
   fills run, not how big they are; `N × 512 MiB` is not a bound an operator can
   act on.
4. **The shared fill outlives its originating request.** Detaching cancellation
   is the only way one client's disconnect stops killing every other waiter.

### Proposed, pending review

These two are judgement calls with a defensible alternative. Flagging rather than
burying them.

- **`filename*` only, no ASCII `filename=` fallback.** RFC 6266 §4.3 recommends
  emitting both. `upload-service/handler.go:483` emits only `filename*`, and
  `docs/client-api.md:787` documents that form. Following the repo over the RFC:
  these artifacts are fetched by an updater that names files from the URL path,
  not by a browser. *Alternative: emit both.*
- **`CACHE_MAX_FILL_BYTES` defaults to 512 MiB**, equal to
  `CACHE_MAX_OBJECT_BYTES` — one max-size fill at a time. *Alternative: a
  multiple of the object cap, trading heap for cache warm-up under burst.*

---

## 1. Bucket — remove creation, keep the probe

`minioutil.ObjectStore` already carries `BucketExists`. The local `bucketClient`
interface and the `bc, ok := minioClient.(bucketClient)` assertion in `main.go`
exist **only** to reach `MakeBucket`, so removing creation removes the scaffold
with it.

```go
// store_minio.go — replaces ensureBucket
func requireBucket(ctx context.Context, client minioutil.ObjectStore, name string) error {
	exists, err := client.BucketExists(ctx, name)
	if err != nil {
		return fmt.Errorf("check bucket %q: %w", name, err)
	}
	if !exists {
		return fmt.Errorf("bucket %q does not exist", name)
	}
	return nil
}
```

`main.go` loses six lines (the type assertion and its error branch) and calls
`requireBucket(ctx, minioClient, cfg.MinioBucket)` directly.

**Operational consequence.** A fresh local stack has no `chat-updates` bucket —
`docker-local/compose.deps.yaml` runs MinIO with no init step, and no service in
the repo bootstraps a bucket. This is now a documented one-time dev setup step in
`client-update-service/deploy/docker-compose.yml`, consistent with how ops/IaC
already owns JetStream streams.

**Risk accepted.** If a managed MinIO scopes the service account to object
operations only, `HeadBucket` may be denied and the pod crash-loops. Judged
unlikely — the same credential already needs bucket-scoped `GetObject`/`PutObject`
— and loud at rollout rather than silent.

## 2. `Content-Disposition` — RFC 5987

`validFileName` accepts any non-empty name without `/`, `\`, or `..`, so CJK,
accented and spaced filenames all reach the header. Today they are interpolated
raw into `filename=%q`, which is only defined for ASCII.

```go
// version.go — ports upload-service/handler.go:483
func contentDisposition(fileName string) string {
	encoded := strings.ReplaceAll(url.QueryEscape(fileName), "+", "%20")
	return fmt.Sprintf("attachment; filename*=UTF-8''%s", encoded)
}
```

`url.QueryEscape` encodes space as `+`, which RFC 5987 does not accept; the
replacement is the reason upload-service's helper has that line. The empty-name
branch upload-service carries is **not** ported: `validFileName` rejects `""`
before either call site, so the branch would be unreachable and uncoverable.

Both call sites (`streamObject`, `serveBytes`) already route through this helper,
so no other change is needed.

## 3. Bounded cache-fill memory

### The defect

`2026-08-03-client-update-service-design.md:27` claims *"Worst-case memory ≤
`CACHE_MAX_ENTRIES × CACHE_MAX_OBJECT_BYTES`"*. That claim is false, and this is
the thread that falsified it.

The LRU bounds **resident** entries. Nothing bounds **transient** fills:

```go
// version.go — loadObject, today
if info.Size > h.cache.maxObjectBytes {
	return cachedBlob{contentType: info.ContentType}, false, nil
}
body := make([]byte, info.Size)   // unaccounted, up to 512 MiB, per concurrent key
```

`singleflight` collapses concurrent misses **per key**, so N requests for one
artifact allocate once. N requests for N *distinct* artifacts open N flights and
allocate N times, concurrently. The LRU's 4-entry cap is consulted only at
`c.add`, after every one of those allocations already happened.

Peak heap is therefore `(concurrently-filling distinct keys) × (size ≤ 512 MiB)`,
with nothing bounding the first factor. Ten concurrent 400 MiB artifacts is
4 GiB. Objects *just under* the object cap are the dangerous ones; anything over
it already streams.

Exposure is limited by two existing properties: `Open` returns
`ErrObjectNotFound` before allocating, so keys must name real objects (an
attacker cannot spray random names — the bound is the number of artifacts in the
bucket, which grows as versions accumulate); and the download endpoint is
unauthenticated by design, so nothing gates concurrency.

### The fix

Reuse the "don't cache this one" exit that already exists. `loadObject`'s
oversized branch returns `cacheable=false`, and `HandleDownload` already responds
to that by calling `streamObject`, which pipes MinIO → client through `io.Copy`'s
32 KB buffer — constant heap regardless of artifact size. The escape hatch is
built and tested; it just needs a second trigger.

```go
// cache.go — new
func (c *blobCache) reserveFill(n int64) bool {
	for {
		cur := c.inFlight.Load()
		if cur+n > c.maxFillBytes {
			return false
		}
		if c.inFlight.CompareAndSwap(cur, cur+n) {
			return true
		}
		// Lost the race; another fill moved the counter. Retry.
	}
}

func (c *blobCache) releaseFill(n int64) { c.inFlight.Add(-n) }
```

The CAS loop makes check-then-add indivisible. A plain
`if load()+n <= max { add(n) }` lets two goroutines both observe 0 and both
allocate — the exact race being fixed.

```go
// version.go — loadObject, four added lines
if info.Size > h.cache.maxObjectBytes {
	return cachedBlob{contentType: info.ContentType}, false, nil
}
if !h.cache.reserveFill(info.Size) {
	return cachedBlob{contentType: info.ContentType}, false, nil   // same exit as oversized
}
defer h.cache.releaseFill(info.Size)
body := make([]byte, info.Size)
```

The reservation cannot be taken earlier: `info.Size` is unknown until `Open`
returns. The `defer` releases on the `io.ReadFull` error path too.

### Worked example — budget 512 MiB, three concurrent distinct 400 MiB artifacts

| | today | with the budget |
|---|---|---|
| A | `make([]byte, 400Mi)` | reserve 400 → in-flight **400** — buffers, gets cached |
| B | `make([]byte, 400Mi)` | 400+400 > 512 — **streams**, ~32 KB |
| C | `make([]byte, 400Mi)` | — **streams**, ~32 KB |
| **peak heap** | **1.2 GiB** | **~400 MiB** |

When A finishes, `releaseFill` returns the budget and the next request buffers
again. Under burst the service degrades to streaming; when the burst passes,
caching resumes unaided. Nothing is rejected and no request is slower than it
would have been with no cache at all.

### The corrected memory bound

```
resident (LRU)    = CACHE_MAX_ENTRIES × CACHE_MAX_OBJECT_BYTES  = 4 × 512Mi = 2 GiB
transient (fills) = CACHE_MAX_FILL_BYTES                        =     512Mi
                                                          total ≈ 2.5 GiB, hard
```

Higher than the number #187 advertised, and unlike that number it is true.
`2026-08-03-client-update-service-design.md:27` and `:166` are corrected in the
same PR, since leaving a falsified claim in a spec is how the next reader repeats
the mistake.

### Configuration

`CACHE_MAX_FILL_BYTES`, int64, default `536870912`, parsed through the existing
`parseByteSize` so float forms still start the service.

`main.go` refuses to start when `CACHE_MAX_FILL_BYTES < CACHE_MAX_OBJECT_BYTES`.
Otherwise an object the size cap deems cacheable could never fit the budget even
on an idle server — it would silently never cache, and the service would look
mysteriously slow. Same fail-fast shape as the `ROOM_KEY_RETIRED_TTL` guard.

## 4. Detaching the coalesced fill

### The defect

`loadCacheable` runs the loader inside `sf.Do`, and the loader closes over the
request context of whichever caller happened to execute the flight. When that
client disconnects, MinIO sees the cancellation and **every** waiter sharing the
flight gets the same error — including waiters whose own connections are alive.
Coalescing turns one disconnect into a fan-out failure.

### The fix

Two independent changes, one for the fill and one for the waiters.

```go
func (c *blobCache) loadCacheable(
	ctx context.Context,
	key string,
	loader func(context.Context) (cachedBlob, bool, error),
) (cachedBlob, bool, error) {
	genBefore := c.gen.Load()
	fillCtx := context.WithoutCancel(ctx)

	ch := c.sf.DoChan(key, func() (any, error) {
		if b, ok := c.get(key); ok {
			return result{blob: b, cacheable: true}, nil
		}
		blob, cacheable, err := loader(fillCtx)
		...
	})

	select {
	case r := <-ch:
		...
	case <-ctx.Done():
		return cachedBlob{}, false, ctx.Err()   // this waiter leaves; the flight lives
	}
}
```

- `context.WithoutCancel` drops cancellation but **keeps context values**, so
  OTel span context still propagates and the MinIO call stays traced.
- The fill remains bounded: `minioVersionStore.Open` applies its own
  `context.WithTimeout(ctx, downloadTimeout)` (`MINIO_DOWNLOAD_TIMEOUT`, 5m), so
  detaching removes the client's cancellation, not the ceiling.
- `DoChan` lets a waiter abandon on its own `ctx.Done()` without touching the
  shared flight.

`HandleDownload` returns without calling `errhttp.Write` when the request context
is done — there is no socket left to write to, and `Classify` would log an
error-level line for a client that merely hung up.

The `gen` invalidation check is unchanged: `genBefore` is still sampled before
the flight and compared before `c.add`.

**Accepted trade-off.** If every waiter leaves, the fill runs to completion and
populates the cache for nobody. Bounded by `MINIO_DOWNLOAD_TIMEOUT` and by the
item-3 budget, and it warms the cache for the next request — cheaper than the
current behaviour of failing live waiters.

**Interaction with item 3.** The fill budget is released when the *fill*
completes, not when the originating request ends. A detached fill therefore holds
its reservation for its full lifetime, which is the correct accounting: the bytes
are live for exactly that long.

---

## Testing

TDD per CLAUDE.md §4 — every test below is written red first and confirmed
failing before implementation.

| Item | Tests |
|---|---|
| 1 | `requireBucket`: present → nil, absent → error, probe error → wrapped. Integration: startup fails against a missing bucket |
| 2 | Table-driven `contentDisposition`: ASCII, spaces, CJK, accented, `%`/quote metacharacters — exact header string asserted |
| 3 | `reserveFill`/`releaseFill` unit table incl. exact-fit and over-budget; concurrent reserve under `-race` never exceeds the budget; handler test: two concurrent distinct keys, second returns `cacheable=false` and streams; **bound test** via `testutil.PeakHeapDuring` asserting peak heap under N distinct concurrent fills |
| 4 | Waiter A cancels mid-flight → waiter B still receives the full body, loader ran exactly once; disconnected waiter gets `ctx.Err()` and writes no response |
| config | `CACHE_MAX_FILL_BYTES` parse (int and float forms); startup rejects `fill < object` |

The item-3 bound test asserts **the bound, not the mechanism**, so any future
change that reintroduces unbounded fills fails it regardless of how. It is a
plain unit test: `testutil.PeakHeapDuring` lives in `pkg/testutil/memtest.go`
with no build tag, and `client-update-service/version_test.go` already imports it.

Gates before commit: `make lint`, `make test SERVICE=client-update-service`,
`make sast`. Integration tests require Docker; run if available, reported
honestly either way.

## Files

| File | Change |
|---|---|
| `client-update-service/store_minio.go` | `ensureBucket` + `bucketClient` → `requireBucket` |
| `client-update-service/main.go` | drop type assertion; `requireBucket`; fill-budget validation |
| `client-update-service/version.go` | RFC 5987 `contentDisposition`; reserve/release in `loadObject`; ctx-aware loader; canceled-waiter handling |
| `client-update-service/cache.go` | `maxFillBytes` + `inFlight`; `reserveFill`/`releaseFill`; `DoChan` + `WithoutCancel` |
| `client-update-service/config.go` | `CACHE_MAX_FILL_BYTES` |
| `client-update-service/deploy/docker-compose.yml` | new knob; note on pre-creating the bucket |
| `*_test.go` (5 files) | as above |
| `docs/client-api.md` | §12 `Content-Disposition` form; cache-fill behaviour under pressure; new knob |
| `docs/client-api/request-reply.md` | keep the derived view in step |
| `docs/superpowers/specs/2026-08-03-client-update-service-design.md` | correct the falsified memory bound |

## Rollout

No migration, no data change, no API break for well-behaved clients.

One behavioural change a deployment must be ready for: **a missing bucket is now
fatal at startup instead of self-healing.** Verify `MINIO_BUCKET` exists in every
environment before rolling out.

`Content-Disposition` moves from `filename="x"` to `filename*=UTF-8''x`. Any
consumer parsing the old form specifically would need updating — none is known,
and the canonical name is in the URL path either way.

`CACHE_MAX_FILL_BYTES` has a working default; no deployment must set it.
