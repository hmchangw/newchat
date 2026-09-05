// Package testdata holds the fixture for rpcmethod.yml.
//
// See metrics.go's header for why this file exists at all: the rule is the
// only gate that catches a transposed string literal at compile time, and a
// pattern edit could silently narrow it without any later scan failing.
//
// `semgrep scan --test` reads the annotations: a `ruleid:` comment names the
// rule that must fire on the following line; an unannotated line is a
// negative assertion — a rule firing there is reported as a false positive.
package testdata

import (
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// The hole this closes: RPCMethod is a named string type, so a typed value
// (natsmetrics.MethodX) cannot be transposed with the pattern argument — the
// compiler already refuses that. What it does not refuse is an untyped string
// constant (assignable to both string and RPCMethod), an explicit
// RPCMethod(...) conversion of any string, or an indirection through a
// variable. All three compile and degrade to rpc_method="_OTHER" at runtime.
//
// The negative cases below therefore cover all three shapes, not just the
// literal. The literal-only version of this rule passed both the conversion
// and the variable.
func registerCallSites(r *natsrouter.Router) {
	// ruleid: rpcmethod-server-route-must-name-a-vocabulary-constant
	natsrouter.Register(r, "chat.user.{account}.request.room.{roomID}.site-a.rename", "rename_room", nil)

	// The generic form, explicit type arguments, is how bot-message-handler
	// and bot-room-service call Register — the type-argument list sits
	// between the function name and the call parens, so it needs its own
	// pattern branch or this case walks straight past the rule.
	// ruleid: rpcmethod-server-route-must-name-a-vocabulary-constant
	natsrouter.Register[BotSendRoomRequest, BotSendResponse](r, "chat.bot.{account}.msg.send.room", "send_room_message", nil)

	// ruleid: rpcmethod-server-route-must-name-a-vocabulary-constant
	natsrouter.RegisterNoBody(r, "chat.user.{account}.request.room.{roomID}.site-a.open", "open_room", nil)

	// ruleid: rpcmethod-server-route-must-name-a-vocabulary-constant
	natsrouter.RegisterOptionalBody(r, "chat.user.{account}.request.sso.refresh", "refresh_sso_token", nil)

	// An explicit conversion: compiles, names nothing in the vocabulary, and
	// walked straight past the literal-only rule.
	// ruleid: rpcmethod-server-route-must-name-a-vocabulary-constant
	natsrouter.Register(r, "chat.user.{account}.request.room.{roomID}.site-a.rename", natsmetrics.RPCMethod("typo"), nil)

	// A variable, even one holding a legitimate constant. Refused because the
	// rule matches syntax: it cannot tell this apart from a variable holding a
	// typo, so it admits neither.
	method := natsmetrics.MethodRenameRoom
	// ruleid: rpcmethod-server-route-must-name-a-vocabulary-constant
	natsrouter.Register(r, "chat.user.{account}.request.room.{roomID}.site-a.rename", method, nil)

	// The fallback is not a method a route may claim.
	// ruleid: rpcmethod-server-route-must-name-a-vocabulary-constant
	natsrouter.RegisterNoBody(r, "chat.user.{account}.request.room.{roomID}.site-a.open", natsmetrics.MethodOther, nil)

	// The correct form: a natsmetrics.Method* selector. No annotation, so any
	// rule firing here is a false positive.
	natsrouter.Register(r, "chat.user.{account}.request.room.{roomID}.site-a.rename", natsmetrics.MethodRenameRoom, nil)
	natsrouter.Register[BotSendRoomRequest, BotSendResponse](r, "chat.bot.{account}.msg.send.room", natsmetrics.MethodSendRoomMessage, nil)
	natsrouter.RegisterNoBody(r, "chat.user.{account}.request.room.{roomID}.site-a.open", natsmetrics.MethodOpenRoom, nil)
	natsrouter.RegisterOptionalBody(r, "chat.user.{account}.request.sso.refresh", natsmetrics.MethodRefreshSSOToken, nil)

	// RegisterVoid takes no method argument at all — the heartbeat lane it
	// serves records no rpc.server.call.duration sample — so there is no
	// method position for this rule to reach into.
	natsrouter.RegisterVoid(r, "chat.user.{account}.request.presence.hello", nil)
}

type (
	BotSendRoomRequest struct{}
	BotSendResponse    struct{}
)
