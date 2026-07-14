# Easy Per-Service Profiling During Load Generation

**Date:** 2026-06-19
**Status:** Approved
**Approach:** Opt-in pprof surface mounted on the existing health server (`pkg/health`), plus a one-shot container-based capture tool (`make profile`) that snapshots every message-pipeline service over the `chat-local` network into a timestamped host folder.

## Summary

While running load (`tools/loadgen`) against the local stack we want to profile the message-pipeline services with near-zero friction — no per-service port plumbing, no production exposure, and one command to snapshot the whole fleet.

Two pieces:

1. **`pkg/health` gains an opt-in pprof surface.** A functional option `health.WithPprof(enabled bool)` mounts the standard `net/http/pprof` handlers (`/debug/pprof/*`) onto the same mux the health server already builds. Off by default — zero behavior change for callers that don't pass it. Wired into the 9 services that stand up a standalone `health.Serve(...)`.
2. **A one-shot capture tool (`make profile`).** A short-lived container on the `chat-local` network fans out to all 9 services' health ports, fetches CPU / heap / goroutine profiles in parallel, and writes them into a per-run timestamped folder on the host.

No new ports are published to the host. No production exposure (gated, default-off). The pprof handler set mirrors what `tools/loadgen` already exposes.

## Decision

| Decision | Choice |
|----------|--------|
| pprof exposure | Mounted on the **existing** health server via a `health.WithPprof` functional option — no new port, no new server |
| Default state | **Off** (`PPROF_ENABLED` env, `envDefault:"false"`) per service; gated so the operator-exposed health port doesn't leak profiling by default |
| Scope | The 9 services that call `health.Serve(...)` standalone (worker + NATS services) |
| Capture mechanism | One-shot container on the external `chat-local` network (curl-equipped image), driven by a root `make profile` target |
| Profile types | `cpu` (30s default), `heap`, `goroutine` — mutex/block omitted (need `SetMutexProfileFraction`/`SetBlockProfileRate`; future toggle) |
| Output | `profiles/<UTC-timestamp>-<label>/<service>.<type>.pprof` on the host (gitignored) |
| Stack wiring | Each service compose gets `PPROF_ENABLED=${PPROF_ENABLED:-false}`, so `PPROF_ENABLED=true make up` turns it on stack-wide |
| Pyroscope | Documented as a purely-additive future path; not built |

## Architecture

### `pkg/health` change

New file `pkg/health/pprof.go`:

- An internal `serverOptions{ pprof bool }` plus `registerPprof(mux *http.ServeMux)` that mounts the 5 standard handlers — `pprof.Index`, `pprof.Cmdline`, `pprof.Profile`, `pprof.Symbol`, `pprof.Trace` (the same set `tools/loadgen` exposes). `pprof.Index` at `/debug/pprof/` also serves the named profiles (`heap`, `goroutine`, `allocs`, …).
- `newServer` consults the `serverOptions`; when `pprof` is set it calls `registerPprof` on the mux it already builds.

**Signature approach.** The existing entrypoints keep their trailing `checks ...Check` variadic untouched (so the 5 non-target services compile unchanged). pprof is exposed through one thin wrapper rather than a second variadic:

- `health.Serve(addr, timeout, checks ...Check)` — unchanged, no pprof.
- `health.ServeWithPprof(addr, timeout, pprofEnabled bool, checks ...Check)` — sets `serverOptions.pprof` then serves. `ServeWithPprof(addr, t, false, checks...)` is byte-for-byte equivalent to `Serve`.

This keeps a single variadic, needs zero call-site churn for services that don't profile, and gives the 9 target services a one-line swap. Default-off: a service passing `false` (or any caller of plain `Serve`) gets exactly today's behavior — only `/healthz` + `/readyz`. Tests cover both the on (200 at `/debug/pprof/`) and off (404) paths.

### Per-service wiring

Each of the 9 services:

- Adds a config field `PProfEnabled bool \`env:"PPROF_ENABLED" envDefault:"false"\``.
- Swaps `health.Serve(cfg.HealthAddr, 5*time.Second, checks...)` → `health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled, checks...)`.

~2 lines per service. No new server, no new port, no new dependency.

The 9 services (all call `health.Serve` standalone):

| Service | Health port |
|---------|-------------|
| broadcast-worker | `:8081` |
| history-service | `:8081` |
| inbox-worker | `:8081` |
| message-gatekeeper | `:8081` |
| message-worker | `:8081` |
| notification-worker | `:8081` |
| room-service | `:8081` |
| room-worker | `:8081` |
| search-sync-worker | `:8081` |

### Compose wiring

Each target service's `deploy/docker-compose.yml` adds, in the service's `environment:` block:

```yaml
      - PPROF_ENABLED=${PPROF_ENABLED:-false}
```

mirroring the existing `DEBUG_LOG_PAYLOADS=${DEBUG_LOG_PAYLOADS:-false}` precedent. A single `PPROF_ENABLED=true make up` enables profiling stack-wide for one session.

### Capture tool (`make profile`)

`tools/profilecapture/capture.sh` — a small POSIX shell script run inside a one-shot `curlimages/curl`-style container joined to the external `chat-local` network (so it resolves each service by compose name):

- **Target manifest**: a static, readable `name:host` list of the 9 services (each on health port `8081`). No discovery magic.
- For each service, **in parallel**, fetch:
  - `cpu` → `/debug/pprof/profile?seconds=$DURATION`
  - `heap` → `/debug/pprof/heap`
  - `goroutine` → `/debug/pprof/goroutine`
- **Params** (env / make vars): `DURATION` (default `30`s), `LABEL` (optional folder tag), `SERVICES` (override the manifest subset).
- **Output**: writes into a host-bind-mounted `profiles/<UTC-timestamp>-<label>/<service>.<type>.pprof`.

Root Makefile gains a `profile` target that:

1. Checks the `chat-local` network exists (same guard as `obs-up`).
2. Runs the capture container with `-v $(PWD)/profiles:/out` and the script.

Read locally with `go tool pprof -http=:8080 profiles/<run>/<service>.cpu.pprof`, or diff two runs with `-diff_base`.

### Workflow

```
PPROF_ENABLED=true make up                  # stack with pprof on (one session)
make run ...                                # existing load run
make profile DURATION=60 LABEL=baseline     # snapshot all 9 services for 60s
```

## Output layout

```
profiles/
  20260619T141500Z-baseline/
    broadcast-worker.cpu.pprof
    broadcast-worker.heap.pprof
    broadcast-worker.goroutine.pprof
    message-worker.cpu.pprof
    ...
```

`profiles/` is added to `.gitignore`.

## Testing

- **`pkg/health`** (TDD, tests first): a unit test asserting `ServeWithPprof(..., true, ...)` (or the option-on path) returns 200 at `/debug/pprof/` and a representative named profile, and that the option-off path returns 404 there while `/healthz` still returns 200. Keeps the package at its coverage floor.
- **Capture script**: a shell fan-out isn't naturally unit-testable; the script stays tiny and the manifest declarative. The behavior is exercised manually against a running stack.

## Future: continuous profiling (documented, not built)

Because pprof ports are now exposed on every target service, adding **Grafana Pyroscope in pull/scrape mode** later is purely additive: a Pyroscope container in `tools/observability/` plus a scrape config listing the same 9 `name:healthport` targets — no service code change. Captured here so the path is known; out of scope for this design.

## Out of scope

- Mutex/block profiling (needs runtime rate setup).
- Production exposure / auth on the pprof surface (kept default-off; ops owns prod posture).
- Continuous profiling backend (Pyroscope) — see above.
</content>
</invoke>
