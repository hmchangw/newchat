package service

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/natsrouter"
)

// registerTestQueue is the queue group the router under test subscribes with.
// Subsz results are filtered on it so only routes this test registered are
// asserted on.
const registerTestQueue = "user-service-test"

// registeredSubjects boots an embedded in-process NATS server, wires
// RegisterHandlers onto a real Router against it, and returns the NATS
// wildcard subjects the server actually holds subscriptions for.
//
// The server is the observation seam: Router keeps its subscription list
// unexported, but every route it registers becomes a real SUB on the wire, so
// the broker's own view is both accessible and closer to production truth than
// any in-process accessor would be.
func registeredSubjects(t *testing.T, siteID string) []string {
	t.Helper()

	ns, err := natsserver.NewServer(&natsserver.Options{Port: -1})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server did not become ready")
	t.Cleanup(ns.Shutdown)

	nc, err := o11ynats.Connect(context.Background(), ns.ClientURL(), noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	// RegisterHandlers only reads s.siteID and binds method values, so a
	// dependency-free UserService exercises the real registration path.
	svc := &UserService{siteID: siteID}
	svc.RegisterHandlers(natsrouter.New(nc, registerTestQueue))

	// Flush round-trips the connection so every SUB has reached the server
	// before Subsz is read — no polling, no sleep.
	require.NoError(t, nc.NatsConn().Flush())

	sz, err := ns.Subsz(&natsserver.SubszOptions{Subscriptions: true, Limit: 1000})
	require.NoError(t, err)

	subjects := make([]string, 0, len(sz.Subs))
	for _, sub := range sz.Subs {
		if sub.Queue != registerTestQueue {
			continue
		}
		subjects = append(subjects, sub.Subject)
	}
	sort.Strings(subjects)
	return subjects
}

// TestUserService_RegisterHandlers_SubjectSet pins the exact set of NATS
// subjects user-service subscribes to. A dropped, renamed, or accidentally
// added registration fails here rather than shipping silently.
func TestUserService_RegisterHandlers_SubjectSet(t *testing.T) {
	want := []string{
		"chat.server.request.user.site-a.badge.count.batch",
		"chat.user.*.request.user.site-a.apps.categories",
		"chat.user.*.request.user.site-a.apps.list",
		"chat.user.*.request.user.site-a.chatlist.get",
		"chat.user.*.request.user.site-a.chatlist.section.create",
		"chat.user.*.request.user.site-a.chatlist.section.delete",
		"chat.user.*.request.user.site-a.chatlist.section.rename",
		"chat.user.*.request.user.site-a.chatlist.section.reorder",
		"chat.user.*.request.user.site-a.chatlist.section.setsortmode",
		"chat.user.*.request.user.site-a.me",
		"chat.user.*.request.user.site-a.profile.getByName",
		"chat.user.*.request.user.site-a.settings.get",
		"chat.user.*.request.user.site-a.settings.priorityContacts.add",
		"chat.user.*.request.user.site-a.settings.priorityContacts.get",
		"chat.user.*.request.user.site-a.settings.priorityContacts.remove",
		"chat.user.*.request.user.site-a.settings.set",
		"chat.user.*.request.user.site-a.sso.refresh",
		"chat.user.*.request.user.site-a.sso.set",
		"chat.user.*.request.user.site-a.status.getByName",
		"chat.user.*.request.user.site-a.status.set",
		"chat.user.*.request.user.site-a.subscription.count",
		"chat.user.*.request.user.site-a.subscription.getByRoomID",
		"chat.user.*.request.user.site-a.subscription.getChannels",
		"chat.user.*.request.user.site-a.subscription.getDM",
		"chat.user.*.request.user.site-a.subscription.list",
		"chat.user.*.request.user.site-a.subscription.setAppSubscription",
		"chat.user.*.request.user.site-a.thread.list",
		"chat.user.*.request.user.site-a.thread.read.all",
		"chat.user.*.request.user.site-a.thread.unread.summary",
	}

	got := registeredSubjects(t, "site-a")

	assert.ElementsMatch(t, want, got,
		"registered subject set drifted; update this golden set only alongside a deliberate RegisterHandlers change")
}

// TestUserService_RegisterHandlers_SiteScoped guards the doc comment's claim
// that siteID is a literal token in every pattern: a subject that lost its site
// token (or carried a wildcard there) would subscribe this instance to other
// sites' traffic.
func TestUserService_RegisterHandlers_SiteScoped(t *testing.T) {
	const siteID = "zz9-site"

	got := registeredSubjects(t, siteID)
	require.NotEmpty(t, got, "no subjects registered; the seam is not observing RegisterHandlers")

	for _, subj := range got {
		assert.Contains(t, strings.Split(subj, "."), siteID,
			"subject %q does not pin siteID as a literal token — cross-site subscription leak", subj)
	}
}
