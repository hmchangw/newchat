# Optional OIDC issuer-check skip in auth-service

**Date:** 2026-08-14
**Status:** Approved (design)
**Scope:** `pkg/oidc`, `auth-service`

## Problem

In a multi-site deployment, a user homed on site `A` (e.g. `00600000`) can enter
through another site's frontend `B` (e.g. `chat-frontend-00f13000`). The frontend
performs the Keycloak login against **its own** configured issuer (`OIDC_ISSUER_URL`
of site `B`) *before* `portal-service` resolves the user's home site. `portal-service`
then points the client's NATS connection at the home site `A`, so site `A`'s
`auth-service` receives an SSO token whose `iss` is site `B`'s issuer.

`auth-service` validates the SSO token via `pkg/oidc`, which today builds a single
go-oidc verifier bound to one configured issuer and enforces a strict `iss` match.
The cross-site token is therefore rejected with an issuer mismatch, even though it is
a legitimate token minted by a trusted sibling site.

## Key precondition (verified)

All trusted sites currently share **the same Keycloak / the same JWKS signing keys**.
This was confirmed by comparing the sites' `jwks_uri` key sets (`kid` + modulus).

Because the signing keys are shared, a token minted by site `B`'s issuer still passes
**signature** verification against the JWKS that site `A`'s `auth-service` fetched from
its own issuer. The only thing that fails is the `iss` **string** comparison. Skipping
that one comparison is sufficient to accept the cross-site token, while signature and
audience validation remain fully in force.

This relaxation is valid **only while the shared-JWKS precondition holds**. If any site
moves to an independent Keycloak (distinct signing keys), skipping the issuer check will
no longer help — signature verification would fail — and a proper multi-issuer design
(per-issuer validator, each fetching its own JWKS, routed by the token's `iss`) would be
required instead. That larger design is explicitly out of scope here.

## Design

A config-gated flag that turns off **only** the `iss` string comparison, following the
existing `TLSSkipVerify` opt-in pattern. Off by default, so every other deployment keeps
strict issuer checking; only a site that explicitly opts in relaxes it.

### `pkg/oidc/oidc.go`

- Add `SkipIssuerCheck bool` to `Config`.
- Pass it through to go-oidc:
  `oidc.Config{SkipClientIDCheck: true, SkipIssuerCheck: cfg.SkipIssuerCheck}`.
- `NewValidator` continues to discover against the single configured `IssuerURL` and to
  fetch that issuer's JWKS — unchanged. Signature and the existing multi-audience
  allow-list check are unchanged.
- A comment at the field and at the apply site documents the shared-JWKS precondition and
  that signature + audience checks still hold.

### `auth-service/main.go`

- Add config field:
  `OIDCSkipIssuerCheck bool \`env:"OIDC_SKIP_ISSUER_CHECK" envDefault:"false"\``.
- Pass it into `pkgoidc.Config{... SkipIssuerCheck: cfg.OIDCSkipIssuerCheck}`.
- When enabled, log once at startup that issuer checking is disabled (audience remains the
  primary trust boundary), so the relaxed posture is visible in service logs.

## Security notes

- **Audience becomes the primary scoping guard.** With `iss` no longer compared, the
  `OIDC_AUDIENCES` allow-list plus the shared signature are the remaining trust anchors.
  Audiences must stay tightly scoped — not widened to accept arbitrary values.
- **Signature is never disabled.** We only set `SkipIssuerCheck`. `InsecureSkipSignatureCheck`
  is never used. A token signed by a key outside the fetched JWKS is still rejected.
- **Off by default and opt-in per site**, mirroring `TLS_SKIP_VERIFY`.
- No gosec rule fires on `SkipIssuerCheck` (it is not a TLS `InsecureSkipVerify`), but the
  intent is documented inline as a deliberate, bounded relaxation.

## Testing (TDD)

Reuse the existing `pkg/oidc/issuer_test.go` `fakeIssuer` helper, which can mint a token
with an overridden `iss` while signing with the same key.

1. **Flag off (default):** token whose `iss` differs from the configured issuer but signed
   by the same key → rejected. (Preserves current behavior.)
2. **Flag on:** the same mismatched-`iss` token → accepted (signature + audience pass).
3. **Flag on + different signing key:** token signed by a key absent from the JWKS →
   still rejected. (Proves signature verification is untouched.)
4. **Flag on + wrong audience:** token with an audience outside the allow-list → still
   rejected. (Proves audience enforcement is untouched.)

## Out of scope / not changed

- No client-facing request/response schema changes → `docs/client-api.md` is not touched.
- No frontend or `portal-service` changes; the home-site handoff flow is unchanged.
- No multi-issuer / per-issuer-JWKS support (would be a separate design if the shared-JWKS
  precondition ever stops holding).

## Files touched

- `pkg/oidc/oidc.go` — `Config.SkipIssuerCheck`, verifier wiring, comments.
- `pkg/oidc/oidc_test.go` (and/or `issuer_test.go`) — the four cases above.
- `auth-service/main.go` — `OIDC_SKIP_ISSUER_CHECK` env + pass-through + startup log.
