package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// historyStore tags every error out of the Cassandra store as a history failure,
// so settle can classify it and the site's degraded marker can be set.
//
// Wrapping here rather than at each call site is deliberate. Store is the Cassandra
// persistence surface by definition, so every error it returns is by construction a
// history failure — deciding that per return statement means one forgotten wrap
// silently routes an outage down the untagged path: NAK forever with no marker, no
// CQL classification and no class-labelled metric, leaving history-service telling
// every client that history is complete while writes are failing. That is the hole
// this whole package exists to close, so it should not be reopenable by omission.
//
// Not every consumer wants this: teams-mode batch migration runs on its own stream,
// consumer and durable, and a bulk-migration persist failure must not tell every live
// client on the site that their history is incomplete. teamsBatchHandler is therefore
// wired with the bare store — the exception is stated by the wiring in main.go rather
// than by a comment asking future readers not to add a wrap.
type historyStore struct{ Store }

func (s historyStore) SaveMessage(ctx context.Context, msg *model.Message, sender *cassParticipant, siteID string) error {
	if err := s.Store.SaveMessage(ctx, msg, sender, siteID); err != nil {
		return historyWriteError{err}
	}
	return nil
}

func (s historyStore) SaveThreadMessage(ctx context.Context, msg *model.Message, sender *cassParticipant, siteID, threadRoomID string) (*int, error) {
	tcount, err := s.Store.SaveThreadMessage(ctx, msg, sender, siteID, threadRoomID)
	if err != nil {
		return nil, historyWriteError{err}
	}
	return tcount, nil
}

func (s historyStore) GetMessageSender(ctx context.Context, messageID string) (*cassParticipant, error) {
	sender, err := s.Store.GetMessageSender(ctx, messageID)
	if err != nil {
		return nil, historyWriteError{err}
	}
	return sender, nil
}

func (s historyStore) GetQuotedParentSnapshot(ctx context.Context, messageID string) (*cassandra.QuotedParentMessage, bool, error) {
	snap, found, err := s.Store.GetQuotedParentSnapshot(ctx, messageID)
	if err != nil {
		return nil, false, historyWriteError{err}
	}
	return snap, found, nil
}

// GetMessageCreatedAt tags a failed read, but never a clean miss. found=false means
// the parent's own canonical write has not landed yet — an ordering race between
// concurrent workers, not evidence that Cassandra is unwell — and the caller returns
// an untagged error for it so one routine race cannot flip a site-wide marker.
func (s historyStore) GetMessageCreatedAt(ctx context.Context, messageID string) (time.Time, bool, error) {
	createdAt, found, err := s.Store.GetMessageCreatedAt(ctx, messageID)
	if err != nil {
		return time.Time{}, false, historyWriteError{err}
	}
	return createdAt, found, nil
}

func (s historyStore) UpdateParentMessageThreadRoomID(ctx context.Context, parentMessageID, roomID string, parentCreatedAt time.Time, threadRoomID string) error {
	if err := s.Store.UpdateParentMessageThreadRoomID(ctx, parentMessageID, roomID, parentCreatedAt, threadRoomID); err != nil {
		return historyWriteError{err}
	}
	return nil
}
