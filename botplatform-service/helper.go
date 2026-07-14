package main

import (
	"github.com/hmchangw/chat/pkg/errcode"
)

// Sentinel errors so handlers can branch with errors.Is and tests can assert
// on the typed error without re-constructing the entire errcode payload.
// Constructing once at package level keeps the reason field aligned with the
// shared pkg/errcode constants.

var (
	// errInvalidCredentials covers unknown account, wrong password, and
	// SSO-only accounts (lacking bot/admin role). Uniform so the wire never
	// reveals which accounts are password-eligible.
	errInvalidCredentials = errcode.Unauthenticated("invalid credentials",
		errcode.WithReason(errcode.BotplatformInvalidCredentials))

	// errInvalidToken is the validate-endpoint rejection when no session row
	// matches the supplied authToken hash.
	errInvalidToken = errcode.Unauthenticated("invalid token",
		errcode.WithReason(errcode.BotplatformInvalidToken))

	// errMissingFields is the 400 for empty username/password/authToken.
	errMissingFields = errcode.BadRequest("required fields are missing",
		errcode.WithReason(errcode.AuthMissingFields))
)
