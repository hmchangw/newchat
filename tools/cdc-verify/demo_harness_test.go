//go:build demoharness

package main

// Demo harness — the real handler + UI on :8091 with seeded data, no backing services.
// Run: go test -tags demoharness -count=1 -run TestDemoHarness ./tools/cdc-verify/

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
)

type demoStats struct{}

func (demoStats) Last() StreamStats {
	return StreamStats{
		Stream:   "MIGRATION-OPLOG-site1",
		Msgs:     48231,
		Bytes:    19_284_042,
		FirstSeq: 1,
		LastSeq:  48231,
		PerSubject: map[string]uint64{
			"chat.migration.oplog.site1.rocketchat_message.insert":      31200,
			"chat.migration.oplog.site1.rocketchat_message.update":      6420,
			"chat.migration.oplog.site1.rocketchat_message.delete":      412,
			"chat.migration.oplog.site1.rocketchat_subscription.update": 7231,
			"chat.migration.oplog.site1.rocketchat_room.update":         1968,
			"chat.migration.oplog.site1.users.update":                   1000,
		},
		RatePerSec: 118.4,
		Consumers: []ConsumerLag{
			{Name: "oplog-transformer", NumPending: 0, AckPending: 3},
			{Name: "oplog-collections-transformer", NumPending: 214, AckPending: 12},
		},
		WatcherLive: true,
		TakenAtMs:   time.Now().UnixMilli(),
	}
}

func demoPairs() []TargetPair {
	return []TargetPair{
		{Source: "rocketchat_message", Alias: "msgById", Kind: "cassandra", Dest: "messages_by_id"},
		{Source: "rocketchat_message", Alias: "msgByRoom", Kind: "cassandra", Dest: "messages_by_room"},
		{Source: "rocketchat_room", Alias: "rooms", Kind: "mongo", Dest: "rooms"},
		{Source: "rocketchat_subscription", Alias: "members", Kind: "mongo", Dest: "room_members"},
		{Source: "rocketchat_subscription", Alias: "subs", Kind: "mongo", Dest: "subscriptions"},
	}
}

type demoInspector struct{}

func (demoInspector) Inspect(_ context.Context, collection, docID string) (InspectResult, error) {
	return InspectResult{
		Collection: collection,
		DocID:      docID,
		Source: map[string]any{
			"_id": docID, "rid": "room-77", "msg": "deploy is green, shipping at 14:00",
			"ts": "2026-08-11T06:02:11Z", "u": map[string]any{"_id": "u_331", "username": "rajeev"},
		},
		SourceFields: []string{"_id", "msg", "rid", "ts", "u._id"},
		Targets: []InspectTarget{
			{Alias: "msgById", Kind: "cassandra", Dest: "messages_by_id",
				Key:           map[string]any{"message_id": docID},
				KeyFields:     []string{"message_id"},
				CompareFields: []string{"created_at", "msg", "sender.account"},
				Doc: map[string]any{"message_id": docID, "room_id": "room-77",
					"msg": "deploy is green, shipping at 14:00", "created_at": 1786428131000,
					"sender": map[string]any{"account": "rajeev"}}},
			{Alias: "msgByRoom", Kind: "cassandra", Dest: "messages_by_room",
				Key:       map[string]any{"room_id": "room-77", "bucket": 1786320000000, "created_at": 1786428131000, "message_id": docID},
				KeyFields: []string{"bucket", "created_at", "message_id", "room_id"},
				Error:     "not-found"},
		},
	}, nil
}

func TestDemoHarness(t *testing.T) {
	hub := newHub()
	results := newResultsStore(200, 1000, hub.broadcastResult)
	now := time.Now().UnixMilli()

	row := func(id, coll, op, doc string, st CheckState, attempts int, dur int64, targets []TargetResult, skip string) CheckResult {
		r := CheckResult{ID: id, Collection: coll, Op: op, DocID: doc, State: st,
			SkipReason: skip, Targets: targets, Attempts: attempts, StartedAtMs: now - dur - 3000}
		if st != StatePending {
			r.EndedAtMs = now - 3000 + dur
		}
		return r
	}

	results.Upsert(row("c9", "rocketchat_room", "delete", "roomX", StateSkipped, 0, 0, nil, "op-skip"))
	results.Upsert(row("c8", "company_hr_acct_org", "update", "hr-88", StateSkipped, 0, 0, nil, "unmapped"))
	results.Upsert(row("c7", "rocketchat_message", "insert", "6VbQ2msg88213", StateMatched, 1, 210, []TargetResult{
		{Alias: "msgById", Matched: true}, {Alias: "msgByRoom", Matched: true}}, ""))
	results.Upsert(row("c6", "rocketchat_subscription", "insert", "sub-3391", StateSuperseded, 2, 4100, []TargetResult{
		{Alias: "subs", Matched: false, LastCause: "dest-missing"},
		{Alias: "members", Matched: false, LastCause: "dest-missing"}}, ""))
	results.Upsert(row("c5", "rocketchat_room", "update", "room-77", StateFailed, 30, 60000, []TargetResult{
		{Alias: "rooms", Matched: false, LastCause: "mismatch", Diffs: []FieldDiff{
			{SourcePath: "name", DestField: "name", Want: "eng-platform", Got: "eng-platfrom"}}}}, ""))
	results.Upsert(row("c4", "rocketchat_message", "insert", "6VbQ2msg88214", StateMatched, 3, 4600, []TargetResult{
		{Alias: "msgById", Matched: true}, {Alias: "msgByRoom", Matched: true}}, ""))
	results.Upsert(row("c3", "rocketchat_subscription", "update", "sub-9021", StateFailed, 30, 60000, []TargetResult{
		{Alias: "subs", Matched: true},
		{Alias: "members", Matched: false, LastCause: "resolver-miss: user"}}, ""))
	results.Upsert(row("c2", "rocketchat_subscription", "update", "sub-4470", StatePending, 4, 0, []TargetResult{
		{Alias: "subs", Matched: true}, {Alias: "members", Matched: false, LastCause: "dest-missing"}}, ""))
	results.Upsert(row("c1", "rocketchat_message", "insert", "6VbQ2msg88215", StateMatched, 1, 180, []TargetResult{
		{Alias: "msgById", Matched: true}, {Alias: "msgByRoom", Matched: true}}, ""))
	results.Upsert(row("c0", "rocketchat_room", "update", "room-81", StateMatched, 2, 2400, []TargetResult{
		{Alias: "rooms", Matched: true}}, ""))

	conn := buildConnInfo(&config{
		SiteID:              "site1",
		NATSURL:             "nats://nats-a.site1.internal:4222",
		CredsFile:           "/etc/nats/verify.creds",
		SourceMongoURI:      "mongodb://admin:secret@rc-mongo-0.legacy:27017,rc-mongo-1.legacy:27017/?replicaSet=rs0",
		SourceMongoUsername: "verify_ro",
		SourceDB:            "rocketchat",
		TargetMongoURI:      "mongodb://chat-mongo.site1.internal:27017",
		TargetMongoUsername: "verify_ro",
		TargetDB:            "chat",
		CassandraHosts:      "cass-0.site1:9042,cass-1.site1:9042,cass-2.site1:9042",
		CassandraKeyspace:   "chat",
		CassandraUsername:   "verify_ro",
		MappingFile:         "/etc/cdc-verify/mapping.json",
	}, "MIGRATION-OPLOG-site1", true)
	rawMapping, err := os.ReadFile("mapping.example.json")
	if err != nil {
		t.Fatalf("read mapping.example.json: %v", err)
	}
	h := newHandler(hub, results, demoStats{}, 200, demoPairs(), demoInspector{}, &conn, rawMapping)
	mux := http.NewServeMux()
	h.registerRoutes(mux)
	ln, err := net.Listen("tcp", ":8091")
	if err != nil {
		t.Fatalf("bind :8091: %v", err)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	fmt.Println("demo harness serving on :8091")
	time.Sleep(1800 * time.Second)
	_ = srv.Close()
}
