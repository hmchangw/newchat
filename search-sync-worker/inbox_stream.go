package main

import (
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

// inboxMemberCollection is the shared base for collections that index
// subscription lifecycle events (member_added, member_removed) off the
// INBOX stream. It centralizes stream config and subject filters so
// spotlight and user-room collections only need to implement the
// index-specific parts.
//
// The stream name + local subject pattern come straight from pkg/stream.Inbox
// so there's one canonical definition for every consumer of INBOX.
// inbox-worker owns INBOX schema bootstrap; cross-site federation (Sources
// + SubjectTransforms) is owned by ops/IaC. search-sync-worker is a pure
// consumer of INBOX.
type inboxMemberCollection struct{}

func (b *inboxMemberCollection) StreamConfig(siteID string) jetstream.StreamConfig {
	c := stream.Inbox(siteID)
	return jetstream.StreamConfig{
		Name:     c.Name,
		Subjects: c.Subjects,
	}
}

func (b *inboxMemberCollection) FilterSubjects(siteID string) []string {
	return subject.InboxMemberEventSubjects(siteID)
}

// StoredScripts returns nil by default. Collections that depend on ES stored
// scripts (user-room) override this; spotlight inherits the nil default since
// it uses plain index/delete actions.
func (b *inboxMemberCollection) StoredScripts() map[string]json.RawMessage {
	return nil
}

// parseMemberEvent decodes an INBOX message into an InboxEvent + its
// InboxMemberEvent payload and validates the common preconditions shared by
// all inbox-member collections.
//
// Callers decide how to handle the event-level restricted-room flag.
// The Go↔painless contract is "positive hss means restricted" — i.e.
// `payload.HistorySharedSince != nil && *payload.HistorySharedSince > 0`;
// a nil pointer OR a leaked `&0`/negative pointer is the intentional
// "unrestricted" sentinel.
//   - user-room routes the event into `restrictedRooms{}` on positive
//     HSS, otherwise into `rooms[]`
//   - spotlight indexes the room regardless of HSS; HSS is a
//     message-content access concern, not a room-name discovery one
func parseMemberEvent(data []byte) (*model.InboxEvent, *model.InboxMemberEvent, error) {
	var evt model.InboxEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return nil, nil, fmt.Errorf("unmarshal inbox event: %w", err)
	}
	if evt.Timestamp <= 0 {
		return nil, nil, fmt.Errorf("parse member event: missing timestamp")
	}
	var payload model.InboxMemberEvent
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return nil, nil, fmt.Errorf("unmarshal inbox member event: %w", err)
	}
	return &evt, &payload, nil
}
