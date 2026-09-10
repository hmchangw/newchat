package natsrouter

import (
	"fmt"
	"regexp"

	"github.com/hmchangw/chat/pkg/natsmetrics"
)

// Route is one subject pattern, the rpc.method its samples carry, and the typed
// handler that serves it.
//
// Routes are data rather than a sequence of Register calls so the set a service
// serves can be validated, golden-filed and printed without parsing source or
// reflecting over handler values. That is also what lets the rpc.method
// vocabulary live here instead of in a hand-kept const block: the set of methods
// the fleet uses is exactly the set its tables declare.
type Route struct {
	Pattern string
	// Method is empty for a Bind that records no sample (HandleVoid), which is
	// why the zero value is natsmetrics.MethodNone rather than a sentinel.
	Method natsmetrics.RPCMethod
	Bind   Binder
}

// Binder defers a typed registration so handlers with different request and
// response types can share one []Route. Handle and its siblings are the only
// constructors, so a Binder always knows whether its shape records a sample —
// which keeps the recording decision on the registration shape, exactly as the
// Register* entry points decide it.
type Binder struct {
	records bool
	bind    func(r *Router, pattern string, method natsmetrics.RPCMethod)
}

// Handle binds a typed request/reply handler.
func Handle[Req, Resp any](fn func(c *Context, req Req) (*Resp, error)) Binder {
	return Binder{
		records: true,
		bind: func(r *Router, pattern string, method natsmetrics.RPCMethod) {
			Register(r, pattern, method, fn)
		},
	}
}

// HandleNoBody binds a handler that takes no request body.
func HandleNoBody[Resp any](fn func(c *Context) (*Resp, error)) Binder {
	return Binder{
		records: true,
		bind: func(r *Router, pattern string, method natsmetrics.RPCMethod) {
			RegisterNoBody(r, pattern, method, fn)
		},
	}
}

// HandleOptionalBody binds a handler that treats a zero-length payload as the
// zero-value request.
func HandleOptionalBody[Req, Resp any](fn func(c *Context, req Req) (*Resp, error)) Binder {
	return Binder{
		records: true,
		bind: func(r *Router, pattern string, method natsmetrics.RPCMethod) {
			RegisterOptionalBody(r, pattern, method, fn)
		},
	}
}

// HandleVoid binds a handler that replies to nothing and records no sample.
func HandleVoid[Req any](fn func(c *Context, req Req) error) Binder {
	return Binder{
		bind: func(r *Router, pattern string, _ natsmetrics.RPCMethod) {
			RegisterVoid(r, pattern, fn)
		},
	}
}

// snakeCaseMethod is the shape an rpc.method label must take. It replaces the
// closed const vocabulary: a table row is checked against the naming rule rather
// than against a list that has to be edited in three places to add a route.
var snakeCaseMethod = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)

// ValidateRoutes reports the first defect in a service's route table.
//
// Uniqueness is checked per table, not fleet-wide: a service cannot see its
// peers' tables. Cross-service collisions are caught by the fleet golden test,
// which reads every service's generated table.
func ValidateRoutes(routes []Route) error {
	methods := make(map[natsmetrics.RPCMethod]struct{}, len(routes))
	patterns := make(map[string]struct{}, len(routes))

	for _, rt := range routes {
		switch {
		case rt.Pattern == "":
			return fmt.Errorf("natsrouter: route with rpc.method %q has an empty pattern", rt.Method)
		case rt.Bind.bind == nil:
			return fmt.Errorf("natsrouter: route %q has no handler", rt.Pattern)
		case rt.Bind.records && rt.Method == natsmetrics.MethodNone:
			return fmt.Errorf("natsrouter: route %q declares no rpc.method", rt.Pattern)
		case !rt.Bind.records && rt.Method != natsmetrics.MethodNone:
			return fmt.Errorf("natsrouter: route %q records no sample but declares rpc.method %q", rt.Pattern, rt.Method)
		case rt.Bind.records && !snakeCaseMethod.MatchString(string(rt.Method)):
			return fmt.Errorf("natsrouter: route %q declares rpc.method %q, which is not snake_case", rt.Pattern, rt.Method)
		}

		if _, dup := patterns[rt.Pattern]; dup {
			return fmt.Errorf("natsrouter: duplicate pattern %q", rt.Pattern)
		}
		patterns[rt.Pattern] = struct{}{}

		if rt.Method == natsmetrics.MethodNone {
			continue
		}
		if _, dup := methods[rt.Method]; dup {
			return fmt.Errorf("natsrouter: duplicate rpc.method %q", rt.Method)
		}
		methods[rt.Method] = struct{}{}
	}

	return nil
}

// RegisterRoutes validates the table and subscribes every route on it.
// It panics on an invalid table for the same reason addRoute panics on a failed
// subscription: this runs once at startup, and a service that cannot serve its
// declared routes must not reach a ready state.
func (r *Router) RegisterRoutes(routes ...Route) {
	if err := ValidateRoutes(routes); err != nil {
		panic(err)
	}
	for _, rt := range routes {
		rt.Bind.bind(r, rt.Pattern, rt.Method)
	}
}
