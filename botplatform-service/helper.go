package main

import (
	"github.com/hmchangw/chat/pkg/errcode"
)

// Package-level sentinels so handlers can errors.Is and tests can assert without reconstructing.

var (
	// errInvalidCredentials covers unknown account, wrong password, and SSO-only accounts.
	// Uniform so the wire never reveals which accounts are password-eligible.
	errInvalidCredentials = errcode.Unauthenticated("invalid credentials",
		errcode.WithReason(errcode.BotplatformInvalidCredentials))

	errInvalidToken = errcode.Unauthenticated("invalid token",
		errcode.WithReason(errcode.BotplatformInvalidToken))

	errMissingFields = errcode.BadRequest("required fields are missing",
		errcode.WithReason(errcode.AuthMissingFields))

	// errBotInvalidToken covers missing token, unknown/expired session, and userId mismatch — unified so the wire doesn't leak which.
	errBotInvalidToken = errcode.Unauthenticated("invalid session token",
		errcode.WithReason(errcode.BotplatformInvalidToken))

	errBotNotABot = errcode.Forbidden("bot role required",
		errcode.WithReason(errcode.BotNotABot))

	// errBotRateLimitedCaller / Global — 429; callers set Retry-After alongside.
	errBotRateLimitedCaller = errcode.TooManyRequests("caller rate limit exceeded",
		errcode.WithReason(errcode.BotRateLimitedCaller))
	errBotRateLimitedGlobal = errcode.TooManyRequests("global rate limit exceeded",
		errcode.WithReason(errcode.BotRateLimitedGlobal))

	// errBotInFlight — 409 for an already-in-flight duplicate opID; callers set Retry-After: 1.
	errBotInFlight = errcode.Conflict("duplicate request in flight",
		errcode.WithReason(errcode.BotInFlight))
)
