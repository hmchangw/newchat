package failoverlane

import (
	"context"
	"fmt"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// RouterFor builds one lane's router: it constructs that lane's NATS-bound
// dependencies from conn/js and returns a router built over conn with the
// service's handlers registered on it.
//
// The home lane is always built first and the failover lane only after it
// returns, so a builder may capture home-lane state (a handler, a service) and
// use it while building the failover lane. Nothing builds the failover lane if
// the home build fails.
//
// It is called once per lane, never shared between them. A lane's replies,
// publishes and outbound RPCs all have to leave on the connection its requests
// arrived on — the failover lane runs precisely because the home cluster is
// unreachable, so anything inherited from the home lane would answer into a dead
// connection.
type RouterFor func(ctx context.Context, conn *o11ynats.Conn, js o11ynats.JetStream, lane subject.Lane) (*natsrouter.Router, error)

// Routers is a request/reply service's per-lane routers.
type Routers struct {
	// Home serves this site's own cluster and is always present.
	Home *natsrouter.Router
	// Buddy serves clients displaced onto the buddy cluster by this site's NATS
	// outage. Nil when no buddy is configured or the lane could not be bound.
	Buddy     *natsrouter.Router
	buddyConn *o11ynats.Conn
}

// BindRouters builds the home router and, when a buddy is configured, an
// equivalent one on the buddy connection.
//
// A site's request subjects are site-scoped, so the buddy site's own instance of
// the service is not subscribed to them: a displaced client's request can still
// only be answered by this site's instance, against this site's databases. What
// the second router changes is where that client can reach it.
//
// Only the home lane is fatal. A buddy that is unconfigured, unreachable, or
// whose build fails leaves the service running home-only, which is both the
// correct single-site behaviour and better than refusing to boot over a cluster
// needed only during an outage.
func BindRouters(ctx context.Context, home *o11ynats.Conn, homeJS o11ynats.JetStream,
	buddy *natsutil.BuddyDialer, build RouterFor,
) (*Routers, error) {
	homeRouter, err := build(ctx, home, homeJS, subject.LaneHome)
	if err != nil {
		return nil, fmt.Errorf("build home router: %w", err)
	}
	routers := &Routers{Home: homeRouter}
	routers.buddyConn = buddy.Bind(ctx,
		func(ctx context.Context, bconn *o11ynats.Conn, bjs o11ynats.JetStream) error {
			buddyRouter, bErr := build(ctx, bconn, bjs, subject.LaneFailover)
			if bErr != nil {
				return fmt.Errorf("build failover router: %w", bErr)
			}
			routers.Buddy = buddyRouter
			return nil
		})
	return routers, nil
}

// ShutdownHooks stops both routers and drains the buddy connection, in the order
// shutdown.Wait should run them: no new requests are accepted on either lane
// before either connection drains, so an in-flight handler on one lane cannot be
// cut off by the other's teardown.
//
// The home connection is not drained here — services drain it alongside their
// own stores, whose ordering only they know.
func (r *Routers) ShutdownHooks() []func(context.Context) error {
	return []func(context.Context) error{
		func(ctx context.Context) error {
			if r.Buddy == nil {
				return nil
			}
			return r.Buddy.Shutdown(ctx)
		},
		func(ctx context.Context) error {
			if r.Home == nil {
				return nil
			}
			return r.Home.Shutdown(ctx)
		},
		natsutil.DrainBuddy(r.buddyConn),
	}
}
