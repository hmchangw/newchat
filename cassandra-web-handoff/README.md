# cassandra-web source handoff

**This branch is a transport, not a change. Do not merge it. Delete it once the
fork PR is open.**

It carries two commits for
[Joey0538/cassandra-web](https://github.com/Joey0538/cassandra-web) as a git
bundle, because the session that produced them could not push there: the git
proxy only injects credentials for repositories in a session's authorized set,
and a session already holding `hmchangw/newchat` cannot add a repo under a
different owner (`add_repo: cross-tier adds are not supported in v1`).

This repository is public, so a session rooted on the fork can read the bundle
anonymously even though it cannot read this repo's private API.

## Recovering the commits

```bash
git clone https://github.com/Joey0538/cassandra-web
cd cassandra-web

# Anonymous read of this branch, just for the bundle.
git clone --depth 1 -b claude/cassandra-web-source-handoff \
  https://github.com/hmchangw/newchat /tmp/handoff

git fetch /tmp/handoff/cassandra-web-handoff/cassandra-web-go1.26-frozen-map-fix.bundle \
  HEAD:go1.26-frozen-map-fix
git push -u origin go1.26-frozen-map-fix
```

The bundle applies onto `665507c`, which is where the fork's `master` sits, so
it fetches cleanly. `git bundle verify` passes.

## What the two commits do

- **`5dd65e4`** — Decode UDT-keyed maps instead of panicking; build on Go 1.26.8.

  `reactions MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>` cannot be read by
  gocql's untyped row APIs: `goType()` maps a UDT to `map[string]interface{}`
  and a map to `reflect.MapOf(key, elem)`, so a UDT-keyed map asks reflect for
  a map keyed by a Go map. `reflect.MapOf` panics rather than returning an
  error, so even `NewWithError` cannot catch it, and with no recover middleware
  the request died mid-response and the table rendered empty with no error.

  `Unmarshal` checks for the `Unmarshaler` interface before any `goType` call,
  so `CQLValue` decodes from the wire instead. Adds a recover middleware, pins
  `ProtoVersion = 4`, serves `DESCRIBE` over native CQL (the bundled `cqlsh` is
  no longer installable — its tarball 404s and it needs python2, dropped from
  Alpine after 3.16), rewrites the Dockerfile to build from the build context
  on `golang:1.26.8-alpine3.24` and `alpine:3.24.1`, and adds 14 decoder tests.

- **`f0cb73b`** — Bump x/crypto, x/net and x/text; stop patching vendored deps.

  govulncheck on the Go 1.26.8 binary still reported 40 vulnerabilities the code
  calls, all in transitive `x/crypto@v0.7.0`, `x/net@v0.8.0` and
  `x/text@v0.8.0`. Now 0.

  Note for future maintenance: `vendor/` carried two local patches that
  `go mod vendor` silently deletes, taking the build with it. The 71-line
  `cast.FToStringMapStringE` moved into the repo as `service/castmap.go`, so
  that one is gone for good. The 6-line `gocql.Session.GetHosts` remains and now
  carries a comment saying it must be re-applied after any vendoring or gocql
  bump.

- **`c34d6c6`** — Patch the runtime image's OpenSSL.

  `alpine:3.24.1` ships openssl 3.5.7-r0, which CVE-2026-63073 (CMP response
  format string, 9.8) and CVE-2026-75803 (ChaCha20-Poly1305 / AES-OCB
  empty-ciphertext auth-tag bypass, 9.1) both apply to. Alpine fixed both in
  3.5.8-r0, so the final stage upgrades and pins that floor — the floor as well
  as the upgrade, because `apk upgrade` alone would quietly produce an
  unpatched image against a stale mirror.

  Real exposure was minimal: the service is `CGO_ENABLED=0` and never links
  OpenSSL, which is only present because apk-tools and busybox's `ssl_client`
  pull it in. A pinned base tag freezes these packages, so the image wants
  periodic rebuilds regardless.

## Verification already done

Against Cassandra 5.0.9 with real UDT-keyed reaction rows: the published
`ipushc/cassandra-web:v1.1.6` panics at `main.go:541` and drops the connection;
this build returns the rows with reactions decoded, on all four tables, with no
panics. Built and pushed as `josephsylvan/cassandra-web:0.1.0-rc3`
(`git-c34d6c6`) and re-verified from a clean registry pull.

Not verified: the Vue UI does not mount in a headless browser here, identically
with the upstream image, so it says nothing about these commits.
