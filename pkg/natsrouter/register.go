package natsrouter

import (
	"encoding/json"
	"log/slog"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsmetrics"
)

// Register subscribes a typed handler to a subject pattern.
// Handler receives *Context (implements context.Context) and unmarshalled request.
// Panics if subscription fails (startup-only, fatal).
func Register[Req, Resp any](
	r *Router,
	pattern string,
	method natsmetrics.RPCMethod,
	fn func(c *Context, req Req) (*Resp, error),
) {
	handler := HandlerFunc(func(c *Context) {
		var req Req
		if err := json.Unmarshal(c.Msg.Data, &req); err != nil {
			// Cause preserves the parse-error chain for the Classify server log
			// without echoing it to the client (errcode.Error.cause is unexported,
			// never JSON-serialized). The user-facing message stays generic.
			replyErr(c, errcode.BadRequest("invalid request payload", errcode.WithCause(err)))
			return
		}

		resp, err := fn(c, req)
		if err != nil {
			replyErr(c, err)
			return
		}

		c.ReplyJSON(resp)
	})

	r.addRPCRoute(pattern, method, []HandlerFunc{handler})
}

// RegisterNoBody subscribes a handler that takes no request body.
func RegisterNoBody[Resp any](
	r *Router,
	pattern string,
	method natsmetrics.RPCMethod,
	fn func(c *Context) (*Resp, error),
) {
	handler := HandlerFunc(func(c *Context) {
		resp, err := fn(c)
		if err != nil {
			replyErr(c, err)
			return
		}

		c.ReplyJSON(resp)
	})

	r.addRPCRoute(pattern, method, []HandlerFunc{handler})
}

// RegisterOptionalBody is like Register but treats a zero-length payload as the zero-value request instead of a bad_request (e.g. sso.refresh).
func RegisterOptionalBody[Req, Resp any](
	r *Router,
	pattern string,
	method natsmetrics.RPCMethod,
	fn func(c *Context, req Req) (*Resp, error),
) {
	handler := HandlerFunc(func(c *Context) {
		var req Req
		if len(c.Msg.Data) > 0 {
			if err := json.Unmarshal(c.Msg.Data, &req); err != nil {
				replyErr(c, errcode.BadRequest("invalid request payload", errcode.WithCause(err)))
				return
			}
		}

		resp, err := fn(c, req)
		if err != nil {
			replyErr(c, err)
			return
		}

		c.ReplyJSON(resp)
	})

	r.addRPCRoute(pattern, method, []HandlerFunc{handler})
}

// RegisterVoid subscribes a handler that processes a request without replying.
//
// It takes no rpc.method and records no rpc.server.call.duration sample: with
// no reply subject there is no call to time, so timing it as an RPC would
// report local handler cost as a round trip.
//
// The percentile-dilution argument that used to sit here — that the presence
// heartbeat lane would drag every quantile down — held only while every route
// shared one rpc_method label, and this vocabulary removed that. The reason
// to keep these out of the RPC family is the semantic one above, not the
// statistical one.
//
// These routes therefore have no latency signal at all today, which is a gap,
// not a resting state: the presence lane is the fleet's highest-volume
// traffic. The replacement is a separate chat.nats.handler.duration with its
// own bounded operation label and no rpc.method — tracked as a P1 follow-up in
// docs/specs/o11y/nats-metrics-contract.md.
func RegisterVoid[Req any](
	r *Router,
	pattern string,
	fn func(c *Context, req Req) error,
) {
	handler := HandlerFunc(func(c *Context) {
		var req Req
		if err := json.Unmarshal(c.Msg.Data, &req); err != nil {
			c.requestResult = natsmetrics.RequestBadRequest
			slog.ErrorContext(c, "invalid payload in void handler", "error", err, "subject", c.Msg.Subject)
			return
		}

		if err := fn(c, req); err != nil {
			c.requestResult = requestResultFromError(err)
			slog.ErrorContext(c, "void handler error", "error", err, "subject", c.Msg.Subject)
		}
	})

	r.addVoidRoute(pattern, []HandlerFunc{handler})
}

// replyErr classifies err and sends the errcode envelope on the reply subject.
func replyErr(c *Context, err error) {
	c.ReplyError(err)
}
