package main

import (
	"testing"

	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/testutil"
)

// TestRegisteredRoutes runs room-service's real registration table and pins every
// route's rpc.method to the subject pattern that claimed it. See
// testutil.AssertRoutesGolden for what the golden file guards and how to
// regenerate it.
func TestRegisteredRoutes(t *testing.T) {
	r := natsrouter.New(startOtelNATS(t), "room-service")
	(&Handler{siteID: "site-a"}).Register(r)

	testutil.AssertRoutesGolden(t, r.Routes())
}
