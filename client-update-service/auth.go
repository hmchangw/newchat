package main

import (
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/svcjwt"
)

// ctxServiceAccount is where requireServiceAccount parks the verified subject
// for the access log.
const ctxServiceAccount = "service_account"

// tokenVerifier is the slice of *svcjwt.Verifier this middleware needs, declared
// here — at the consumer — so tests can substitute a stub.
type tokenVerifier interface {
	Verify(token string) (*svcjwt.Claims, error)
}

// requireServiceAccount admits a request only when it carries a service token
// this site trusts AND that token names an allowlisted subject.
//
// The two failures are answered differently on purpose. A JWT cannot be
// guessed — forging one needs the private key — so distinguishing "your token
// is bad" (401) from "your account is not permitted" (403) leaks nothing
// exploitable, and turns a missing allowlist entry into an immediately
// diagnosable 403 rather than a mystery 401.
func requireServiceAccount(v tokenVerifier, allowed []string) gin.HandlerFunc {
	permitted := make(map[string]struct{}, len(allowed))
	for _, a := range allowed {
		// Trim so "a, b" from an env var works, and skip blanks so an empty
		// entry can never permit an empty subject.
		if a = strings.TrimSpace(a); a != "" {
			permitted[a] = struct{}{}
		}
	}
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		tok := bearerToken(c)
		if tok == "" {
			errhttp.Write(ctx, c, errcode.Unauthenticated("missing service token",
				errcode.WithReason(errcode.ClientUpdateInvalidToken)))
			c.Abort()
			return
		}

		claims, err := v.Verify(tok)
		if err != nil {
			// The cause names the broken rule for the server log only; Classify
			// logs it once and never serializes it.
			errhttp.Write(ctx, c, errcode.Unauthenticated("invalid service token",
				errcode.WithReason(errcode.ClientUpdateInvalidToken),
				errcode.WithCause(err)))
			c.Abort()
			return
		}

		if _, ok := permitted[claims.Subject]; !ok {
			errhttp.Write(ctx, c, errcode.Forbidden("service account is not authorized to upload",
				errcode.WithReason(errcode.ClientUpdateNotAuthorized)))
			c.Abort()
			return
		}

		c.Set(ctxServiceAccount, claims.Subject)
		c.Next()
	}
}

// bearerToken extracts the token from "Authorization: Bearer <token>", or ""
// when the header is absent or uses another scheme.
func bearerToken(c *gin.Context) string {
	if after, ok := strings.CutPrefix(c.GetHeader("Authorization"), "Bearer "); ok {
		return strings.TrimSpace(after)
	}
	return ""
}
