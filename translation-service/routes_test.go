package main

import (
	"testing"

	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/testutil"
)

// TestRegisteredRoutes runs translation-service's real registration table and pins every
// route's rpc.method to the subject pattern that claimed it. See
// testutil.AssertRoutesGolden for what the golden file guards and how to
// regenerate it.
func TestRegisteredRoutes(t *testing.T) {
	r := natsrouter.New(testutil.EmbeddedNATS(t), "translation-service")
	registerRoutes(r, &Handler{}, "site-a")

	testutil.AssertRoutesGolden(t, r.Routes())
}
