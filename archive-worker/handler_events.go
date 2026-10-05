package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

var (
	errPoison = errors.New("poison event")
	errSkip   = errors.New("event not archived")
)

type msgMeta struct {
	Stream   string
	Seq      uint64
	Subject  string
	StoredAt time.Time // when JetStream stored the message; identical on every delivery
}

func metaOf(msg jetstream.Msg) (msgMeta, error) {
	md, err := msg.Metadata()
	if err != nil {
		return msgMeta{}, fmt.Errorf("%w: metadata: %v", errPoison, err)
	}
	if md.Sequence.Stream == 0 {
		return msgMeta{}, fmt.Errorf("%w: zero stream sequence", errPoison)
	}
	return msgMeta{Stream: md.Stream, Seq: md.Sequence.Stream, Subject: msg.Subject(), StoredAt: md.Timestamp.UTC()}, nil
}

type eventBody struct {
	Content             string                         `json:"content,omitempty"`
	Attachments         [][]byte                       `json:"attachments,omitempty"`
	Card                *cassandra.Card                `json:"card,omitempty"`
	CardAction          *cassandra.CardAction          `json:"cardAction,omitempty"`
	QuotedParentMessage *cassandra.QuotedParentMessage `json:"quotedParentMessage,omitempty"`
}

// eventAt is the event's own timestamp, or when it has none the stream's
// store time. Never the pod clock: a redelivery must rebuild the identical
// record, or its contentHash would no longer match the archived one.
func eventAt(ts int64, storedAt time.Time) time.Time {
	if ts <= 0 {
		return storedAt
	}
	return time.UnixMilli(ts).UTC()
}

// sealRecord encrypts the canonical record into a frame and returns the
// frame plus the keyed record digest the document carries for later verification.
func sealRecord(site string, meta msgMeta, at time.Time, data []byte, c *auditarchive.Cipher) ([]byte, string, error) {
	rec := auditarchive.Record{Site: site, Stream: meta.Stream, Seq: meta.Seq, Subject: meta.Subject, EventAt: at.UnixMilli(), Payload: json.RawMessage(data)}
	plain, err := rec.Marshal()
	if err != nil {
		return nil, "", fmt.Errorf("%w: %v", errPoison, err)
	}
	frame, err := c.Seal(plain, auditarchive.FrameAAD(site, meta.Seq))
	if err != nil {
		return nil, "", fmt.Errorf("seal record: %w", err)
	}
	return frame, c.Digest(plain), nil
}

func buildEventItem(ctx context.Context, site string, msg jetstream.Msg, data []byte, c *auditarchive.Cipher, now time.Time) (item, error) {
	meta, err := metaOf(msg)
	if err != nil {
		return item{}, err
	}
	var ev model.MessageEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		return item{}, fmt.Errorf("%w: unmarshal message event: %v", errPoison, err)
	}
	if ev.Event == "" {
		ev.Event = model.EventCreated
	}
	switch ev.Event {
	case model.EventCreated, model.EventUpdated, model.EventDeleted, model.EventPinned, model.EventUnpinned, model.EventReacted:
	default:
		return item{}, fmt.Errorf("%w: event type %q", errSkip, ev.Event)
	}
	if ev.Message.ID == "" || ev.Message.RoomID == "" {
		return item{}, fmt.Errorf("%w: message id or room id missing", errPoison)
	}
	at := eventAt(ev.Timestamp, meta.StoredAt)
	frame, hash, err := sealRecord(site, meta, at, data, c)
	if err != nil {
		return item{}, err
	}
	doc := &auditarchive.EventDoc{
		Seq: meta.Seq, EventType: string(ev.Event), EventAt: at,
		MessageID: ev.Message.ID, RoomID: ev.Message.RoomID, SiteID: site,
		SenderAccount: ev.Message.UserAccount, SenderID: ev.Message.UserID,
		CreatedAt: ev.Message.CreatedAt.UTC(), ThreadParentID: ev.Message.ThreadParentMessageID,
		ContentHash: hash,
	}
	switch ev.Event {
	case model.EventCreated, model.EventUpdated:
		atts, _ := cassandra.DecodeAttachments(ev.Message.Attachments) // skipped count discarded: undecodable blobs stay in EncBody, only the metadata count omits them
		doc.AttachmentCount = len(atts)
		for i := range atts {
			doc.AttachmentTypes = append(doc.AttachmentTypes, atts[i].FileType)
		}
		body, err := json.Marshal(eventBody{Content: ev.Message.Content, Attachments: ev.Message.Attachments, Card: ev.Message.Card, CardAction: ev.Message.CardAction, QuotedParentMessage: ev.Message.QuotedParentMessage})
		if err != nil {
			return item{}, fmt.Errorf("%w: marshal body: %v", errPoison, err)
		}
		if doc.EncBody, err = c.Seal(body, auditarchive.BodyAAD(site, meta.Seq)); err != nil {
			return item{}, fmt.Errorf("seal body: %w", err)
		}
	case model.EventReacted:
		if ev.ReactionDelta != nil {
			doc.ActorAccount = ev.ReactionDelta.Actor.Account
		}
	case model.EventPinned, model.EventUnpinned:
		doc.ActorAccount = ev.Message.UserAccount
		if ev.Message.PinnedBy != nil {
			doc.ActorAccount = ev.Message.PinnedBy.Account
		}
	default:
		doc.ActorAccount = ev.Message.UserAccount
	}
	// The daily index is the archive day, not the event day: a late or
	// redelivered event lands in an index that is still writable (each turns
	// read-only a day after creation), and readers dedup by seq.
	return item{ctx: ctx, msg: msg, seq: meta.Seq, frame: frame, hash: hash, eventAt: at,
		docs: []docSpec{{Index: auditarchive.EventsIndex(site, now), ID: auditarchive.EventDocID(site, meta.Seq), Doc: doc}}}, nil
}
