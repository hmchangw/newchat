---
description: Scan Go code for CLAUDE.md logging violations (slog JSON discipline, request-ID propagation, secret leakage)
argument-hint: [path]
---

You are scanning Go code for logging violations against CLAUDE.md Section 3 ("Logging" + "Request Logging & Tracing"). This is a **report-only** scan — no auto-fixes.

# Scope

- If `$ARGUMENTS` is empty: scan `.go` files changed on this branch — `git diff --name-only main...HEAD` ∪ tracked `.go` files in `git status --porcelain`.
- If `$ARGUMENTS` is a path (file or directory): scan `.go` files under that path recursively.

Skip:
- Generated files: `*_gen.go`, `mock_*.go`, files whose first 5 lines contain `// Code generated`.
- `tools/` CLI binaries where `fmt.Println`/`fmt.Printf` is legitimate stdout/stderr output. Heuristic: if the file's package is `main` AND lives under `tools/`, allow `fmt.Println` and `fmt.Printf` UNLESS they look log-shaped (key=value pairs, timestamps prepended, level prefixes).

# Detection rules

Walk every file in scope. For each violation, capture `severity`, `file:line`, `rule`, and a one-line `suggested fix`.

**`critical` — possible secret leakage**

Log calls (`slog.*`, `log.*`) where any key or value-source name matches: `password`, `passwd`, `pwd`, `token`, `secret`, `apikey`, `api_key`, `bearer`, `authorization`, `credential`, `nkey`, `jwt`, `body`, `payload`, `raw`, `message_body`, `private_key`, `priv_key`.

Special cases: `key` alone is too noisy (context keys like `"user_id"` use that name); only flag `key` when it appears alongside `value` of a credential-shaped type or is part of a clear secret context.

Also flag log calls that dump a whole struct that may contain credentials — e.g. `slog.Info("config loaded", "cfg", cfg)` where `cfg` is a config struct. Verify by reading the type definition.

**`high` — wrong logger**

- `log.Println`, `log.Printf`, `log.Print`, `log.Fatal*`, `log.Panic*` (stdlib `log`, not `log/slog`).
- `fmt.Println` / `fmt.Printf` used for logging — appears inside `handler.go` / `store.go` / `main.go` / worker files outside `tools/`, or includes manually-formatted timestamps / level prefixes.
- `slog.NewTextHandler` instead of `slog.NewJSONHandler` — CLAUDE.md requires JSON format.

**`medium` — request tracing gaps**

- Gin HTTP services missing request-logging middleware. Check that any service exposing HTTP routes (`*.GET`, `*.POST`, etc. in `routes.go` or `main.go`) has middleware logging method / path / status / latency / request-ID.
- Handler functions that accept `context.Context` but never extract a request ID for logging (heuristic: handler logs without a `request_id` or `requestID` field while context is in scope).
- HTTP/NATS entry points that don't use `idgen.GenerateRequestID()` when no inbound `X-Request-ID` header is present.

**`low` — structured logging discipline**

- Interpolated log messages: `slog.Info(fmt.Sprintf("user %s did X", id))` instead of `slog.Info("user did X", "user", id)`.
- Mixed message + concatenation: `slog.Info("user " + id + " did X", ...)`.

**`nitpick`**

- Inconsistent field naming within the same service (e.g. `userID` in one file, `user_id` in another, `user` elsewhere).

# Output

Print a single Markdown table to chat — no report file (this is a fast scan):

```
| Severity | file:line | Rule | Suggested fix |
```

Order rows by severity (critical first, nitpick last), then by file path.

If the table would exceed 30 rows, truncate to the top 30 by severity and add a line: `… <N> additional findings omitted; narrow the scope or run again on a subdir.`

Final summary line: `<N> findings: <C> critical / <H> high / <M> medium / <L> low / <K> nitpick.`

If zero findings: print `No logging violations detected in scope <path-or-branch>.` and stop.

# Do NOT auto-fix

This command reports only. Suggesting fixes inline is fine; applying them is not. The user reviews findings and decides what to fix manually or in a follow-up.
