package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/model"
)

func buildMemberItem(ctx context.Context, site string, msg jetstream.Msg, data []byte, c *auditarchive.Cipher, now time.Time) (item, error) {
	meta, err := metaOf(msg)
	if err != nil {
		return item{}, err
	}
	var outer model.InboxEvent
	if err := json.Unmarshal(data, &outer); err != nil {
		return item{}, fmt.Errorf("%w: unmarshal inbox event: %v", errPoison, err)
	}
	switch outer.Type {
	case model.InboxMemberAdded, model.InboxMemberRemoved, model.InboxRoomRenamed:
	case model.InboxMemberJoinedAtRefreshed:
		return item{}, fmt.Errorf("%w: %s", errSkip, outer.Type)
	default:
		return item{}, fmt.Errorf("%w: inbox type %q", errSkip, outer.Type)
	}
	if len(outer.Payload) == 0 {
		return item{}, fmt.Errorf("%w: empty inbox payload", errPoison)
	}
	var inner model.InboxMemberEvent
	if err := json.Unmarshal(outer.Payload, &inner); err != nil {
		return item{}, fmt.Errorf("%w: unmarshal member event: %v", errPoison, err)
	}
	if inner.RoomID == "" {
		return item{}, fmt.Errorf("%w: member event without room id", errPoison)
	}
	if outer.Type != model.InboxRoomRenamed && len(inner.Accounts) == 0 {
		return item{}, fmt.Errorf("%w: %s without accounts", errPoison, outer.Type)
	}
	at := eventAt(outer.Timestamp, meta.StoredAt)
	frame, hash, err := sealRecord(site, meta, at, data, c)
	if err != nil {
		return item{}, err
	}
	accounts := inner.Accounts
	if outer.Type == model.InboxRoomRenamed {
		accounts = []string{""}
	}
	index := auditarchive.MembersIndex(site, now) // archive day, as for events
	docs := make([]docSpec, 0, len(accounts))
	for i, acct := range accounts {
		docs = append(docs, docSpec{Index: index, ID: auditarchive.MemberDocID(site, meta.Seq, i), Doc: &auditarchive.MemberDoc{
			Seq: meta.Seq, EventType: outer.Type, EventAt: at,
			RoomID: inner.RoomID, RoomSiteID: inner.SiteID, Account: acct,
			RoomType: string(inner.RoomType), RoomName: inner.RoomName, ContentHash: hash,
		}})
	}
	return item{ctx: ctx, msg: msg, seq: meta.Seq, frame: frame, hash: hash, eventAt: at, docs: docs}, nil
}
