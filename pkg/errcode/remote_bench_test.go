package errcode_test

import (
	"encoding/json"
	"testing"

	"github.com/hmchangw/chat/pkg/errcode"
)

// successPayload is a representative hot-path reply: what broadcast-worker and
// message-gatekeeper receive on every threaded message, not a toy object.
var successPayload = func() []byte {
	b, err := json.Marshal(map[string]any{
		"messageId": "01970a4f8c2d7c9aabcde0123456789f",
		"roomId":    "01970a4f8c2d7c9aabcde01234567800",
		"createdAt": "2026-06-01T12:00:00Z",
		"sender":    map[string]any{"id": "u-bob", "account": "bob", "engName": "Bob Chen"},
		"msg":       "the thread's root message, long enough to be representative of a real chat line",
		"mentions":  []string{"alice", "carol", "dave"},
		"attachments": []map[string]any{
			{"name": "spec.pdf", "size": 918273, "url": "https://example.invalid/a"},
		},
	})
	if err != nil {
		panic(err)
	}
	return b
}()

var errorPayload = []byte(`{"code":"not_found","reason":"thread_parent_not_found","error":"message not found"}`)

// The success path is the hot one: FromReply decodes a single json.RawMessage
// field where Parse decodes the whole Error struct, so the extra unmarshals it
// can do on the error path cost nothing here.
func BenchmarkParse_Success(b *testing.B) {
	b.SetBytes(int64(len(successPayload)))
	for b.Loop() {
		if _, ok := errcode.Parse(successPayload); ok {
			b.Fatal("unexpected envelope")
		}
	}
}

func BenchmarkFromReply_Success(b *testing.B) {
	b.SetBytes(int64(len(successPayload)))
	for b.Loop() {
		if err := errcode.FromReply(successPayload); err != nil {
			b.Fatal(err)
		}
	}
}

// The error path pays for the extra passes, on a call that has already spent a
// failed network round trip.
func BenchmarkParse_Error(b *testing.B) {
	for b.Loop() {
		if _, ok := errcode.Parse(errorPayload); !ok {
			b.Fatal("expected envelope")
		}
	}
}

func BenchmarkFromReply_Error(b *testing.B) {
	for b.Loop() {
		if err := errcode.FromReply(errorPayload); err == nil {
			b.Fatal("expected envelope")
		}
	}
}
