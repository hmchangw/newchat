package natsutil_test

import (
	"encoding/json"
	"testing"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

func TestMarshalResponse(t *testing.T) {
	room := model.Room{ID: "1", Name: "general"}
	data, err := natsutil.MarshalResponse(room)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got model.Room
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ID != "1" || got.Name != "general" {
		t.Errorf("got %+v", got)
	}
}

func TestMarshalError(t *testing.T) {
	data := natsutil.MarshalError("something went wrong")
	var got model.ErrorResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Error != "something went wrong" {
		t.Errorf("got %q", got.Error)
	}
}

func TestTryParseError(t *testing.T) {
	t.Run("error body returns parsed response and true", func(t *testing.T) {
		data := natsutil.MarshalError("boom")
		resp, ok := natsutil.TryParseError(data)
		if !ok {
			t.Fatal("expected ok=true for error body")
		}
		if resp.Error != "boom" {
			t.Errorf("got %q, want %q", resp.Error, "boom")
		}
	})

	t.Run("success body with no error field returns false", func(t *testing.T) {
		data, err := json.Marshal(model.ListRoomMembersResponse{Members: nil})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if _, ok := natsutil.TryParseError(data); ok {
			t.Fatal("expected ok=false for success body")
		}
	})

	t.Run("empty object returns false", func(t *testing.T) {
		if _, ok := natsutil.TryParseError([]byte(`{}`)); ok {
			t.Fatal("expected ok=false for {}")
		}
	})

	t.Run("malformed json returns false", func(t *testing.T) {
		if _, ok := natsutil.TryParseError([]byte(`{not json`)); ok {
			t.Fatal("expected ok=false for malformed json")
		}
	})

	t.Run("error field with empty string returns false", func(t *testing.T) {
		// Guards against rogue callers sending {"error":""}; we treat them as success bodies.
		if _, ok := natsutil.TryParseError([]byte(`{"error":""}`)); ok {
			t.Fatal("expected ok=false for empty error string")
		}
	})
}
