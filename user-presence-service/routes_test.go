package main

import (
	"testing"

	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/testutil"
)

// TestRegisteredRoutes runs user-presence-service's real registration table and pins every
// route's rpc.method to the subject pattern that claimed it. See
// testutil.AssertRoutesGolden for what the golden file guards and how to
// regenerate it.
//
// The golden holds three entries, not seven: Hello/Ping/Activity/Bye are
// RegisterVoid routes, which name no rpc.method and so never enter Routes().
func TestRegisteredRoutes(t *testing.T) {
	r := natsrouter.New(testutil.EmbeddedNATS(t), "user-presence-service")
	registerRoutes(r, &Handler{}, "site-a")

	testutil.AssertRoutesGolden(t, r.Routes())
}
