# profilecapture

One-shot pprof snapshot across the message-pipeline services, for profiling the
stack while `tools/loadgen` drives load.

## How it works

Each of the nine pipeline services mounts the standard `net/http/pprof`
endpoints on its existing health port (`:8081`), gated behind `PPROF_ENABLED`
(default `false`). `make profile` runs `capture.sh` inside a short-lived curl
container joined to the `chat-local` network; it fans out to every service,
fetches CPU + heap + goroutine profiles in parallel, and writes them to the
host under `profiles/<UTC-timestamp>[-<label>]/<service>.<type>.pprof`.

No extra ports are published; profiling is off unless explicitly enabled.

## Usage

```sh
# 1. Bring the stack up with pprof enabled (one session).
PPROF_ENABLED=true make up

# 2. Run your load (existing loadgen workflow).
# 3. Snapshot every service.
make profile                       # 30s CPU window
make profile DURATION=60 LABEL=baseline
make profile SERVICES="message-worker broadcast-worker"
```

Tunables: `DURATION` (CPU seconds, default 30), `LABEL` (folder tag),
`SERVICES` (manifest override).

## Reading profiles

```sh
go tool pprof -http=:8080 profiles/<run>/message-worker.cpu.pprof
# Compare two runs:
go tool pprof -http=:8080 -diff_base profiles/<baseline>/message-worker.cpu.pprof \
  profiles/<after>/message-worker.cpu.pprof
```

`profiles/` is gitignored.
