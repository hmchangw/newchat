package main

import (
	"encoding/json"
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
)

// presenceTransition is one outbound presence event plus the effective status
// the service is expected to publish as a result. expect == StatusNone means
// the transition is a no-op at the service (steady-state ping or an activity
// call that doesn't move the aggregate) and must not be measured.
type presenceTransition struct {
	subject string
	payload []byte
	expect  model.PresenceStatus
}

// presenceUser is one synthetic identity with a single logical connection. It
// mirrors the effective status the service should compute so each transition
// can declare its expected resulting publish.
type presenceUser struct {
	idx     int
	account string
	connID  string
	siteID  string
	status  model.PresenceStatus // mirror of effective status
	away    bool                 // current activity (true = inactive)
}

// presenceAccount is the deterministic synthetic account for index i. The
// presence service never validates these against Mongo on hello/ping/activity/
// bye, so no seeding is required.
func presenceAccount(i int) string { return fmt.Sprintf("u-%06d", i) }

// newPresenceUserForAccount builds a presenceUser bound to an explicit account
// (connID = "c-"+account). idx is -1 because there is no synthetic index; this
// is used by daily, whose users carry real fixture accounts. The daily emitter
// never reads idx, so the sentinel is safe.
func newPresenceUserForAccount(account, siteID string) *presenceUser {
	return &presenceUser{
		idx:     -1,
		account: account,
		connID:  "c-" + account,
		siteID:  siteID,
		status:  model.StatusOffline,
	}
}

func newPresenceUser(idx int, siteID string) *presenceUser {
	u := newPresenceUserForAccount(presenceAccount(idx), siteID)
	u.idx = idx
	return u
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		// Payloads are fixed-shape structs from pkg/model; marshal cannot fail.
		panic(fmt.Sprintf("marshal presence payload: %v", err))
	}
	return b
}

func (u *presenceUser) hello(tsMillis int64) presenceTransition {
	u.status = model.StatusOnline
	u.away = false
	return presenceTransition{
		subject: presenceHelloSubject(u.account, u.siteID),
		payload: mustMarshal(model.Hello{ConnID: u.connID, Timestamp: tsMillis}),
		expect:  model.StatusOnline,
	}
}

func (u *presenceUser) ping(tsMillis int64) presenceTransition {
	// A ping for a known connection is a no-op at the service.
	return presenceTransition{
		subject: presencePingSubject(u.account, u.siteID),
		payload: mustMarshal(model.Ping{ConnID: u.connID, Timestamp: tsMillis}),
		expect:  model.StatusNone,
	}
}

func (u *presenceUser) setAway(away bool, tsMillis int64) presenceTransition {
	tr := presenceTransition{
		subject: presenceActivitySubject(u.account, u.siteID),
		payload: mustMarshal(model.Activity{ConnID: u.connID, Away: away, Timestamp: tsMillis}),
		expect:  model.StatusNone,
	}
	// Only an actual change while online moves the published aggregate.
	if u.status != model.StatusOffline && away != u.away {
		if away {
			u.status = model.StatusAway
			tr.expect = model.StatusAway
		} else {
			u.status = model.StatusOnline
			tr.expect = model.StatusOnline
		}
	}
	u.away = away
	return tr
}

func (u *presenceUser) bye(tsMillis int64) presenceTransition {
	u.status = model.StatusOffline
	return presenceTransition{
		subject: presenceByeSubject(u.account, u.siteID),
		payload: mustMarshal(model.ByeRequest{ConnID: u.connID, Timestamp: tsMillis}),
		expect:  model.StatusOffline,
	}
}
