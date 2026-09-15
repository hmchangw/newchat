# cassandra-web: OpenSSL CVE fix, pending on master

**This branch is a transport, not a change. Do not merge it. Delete it once the
fork PR is open.**

The earlier bundle here (three commits) is gone: PR #1 and PR #2 landed on
`Joey0538/cassandra-web` master, so two of those three are already merged. What
is left is the one commit that never made it — the OpenSSL patch — rebased onto
current master (`eaa23f1`).

## The CVEs

`alpine:3.24.1` ships `openssl 3.5.7-r0`. Both of these apply to it, both rated
critical, and Alpine fixed both in **`3.5.8-r0`** (see
https://secdb.alpinelinux.org/v3.24/main.json):

| CVE | CVSS 3.1 | Issue |
|---|---|---|
| CVE-2026-63073 | 9.8 | CMP response validation passes an unexpected sender distinguished name straight to `ERR_raise_data()` as a format string, so a malicious or intercepted CMP endpoint can crash the client |
| CVE-2026-75803 | 9.1 | ChaCha20-Poly1305 and AES-OCB decryption of an *empty* ciphertext can report success without verifying the authentication tag, when finalized via `EVP_Cipher()` |

Real exposure in this image is minimal — the service is built `CGO_ENABLED=0`
and uses Go's own crypto, so it never links OpenSSL. `libcrypto3`/`libssl3` are
present only because `apk-tools` and busybox's `ssl_client` depend on them, and
neither goes near the CMP or `EVP_Cipher` paths these need. This is scanner
hygiene rather than live exposure, but a critical finding is worth clearing.

## Recovering the commit

```bash
git clone https://github.com/Joey0538/cassandra-web && cd cassandra-web

git clone --depth 1 -b claude/cassandra-web-source-handoff \
  https://github.com/hmchangw/newchat /tmp/handoff

git fetch /tmp/handoff/cassandra-web-handoff/openssl-cve-fix.bundle \
  HEAD:openssl-cve-fix
git push -u origin openssl-cve-fix
```

It applies onto `eaa23f1`, which is where master sits, so it fetches cleanly.

## Or just apply it by hand

It is a single hunk in the final stage of the `Dockerfile`, immediately after
`FROM alpine:3.24.1`:

```dockerfile
# Patch the base image's own packages. alpine:3.24.1 ships openssl 3.5.7-r0,
# which CVE-2026-63073 (CMP response format string, 9.8) and CVE-2026-75803
# (ChaCha20-Poly1305 / AES-OCB empty-ciphertext auth-tag bypass, 9.1) apply to.
# Alpine fixed both in 3.5.8-r0.
#
# The version floor is explicit as well as the blanket upgrade: `apk upgrade`
# alone would silently produce an unpatched image if the mirror were stale,
# whereas a floor fails the build instead. Rebuild periodically — a pinned base
# tag freezes these packages at whatever that tag shipped with.
#
# Note the service itself does not link OpenSSL (CGO_ENABLED=0, so Go's own
# crypto). libcrypto3/libssl3 are here because apk-tools and busybox's
# ssl_client depend on them, and neither reaches the CMP or EVP_Cipher paths
# these two CVEs need. This is scanner hygiene, not live exposure.
RUN apk upgrade --no-cache \
    && apk add --no-cache "libcrypto3>=3.5.8-r0" "libssl3>=3.5.8-r0"
```

The floor matters as much as the upgrade: `apk upgrade` on its own would
quietly produce an unpatched image against a stale mirror, whereas the floor
fails the build.

## Verified

Built from master + this commit: `libcrypto3`/`libssl3` are `3.5.8-r0`, the UI
serves, `DESCRIBE` works, and all four Cassandra tables return rows with
reactions decoded, with no panics. The same rebuild also moves `apk-tools`
3.0.6-r0 to 3.0.8-r0. Published as `josephsylvan/cassandra-web:0.1.0`.
