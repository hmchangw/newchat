package natsutil

import (
	"encoding/json"
	"log/slog"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/model"
)

// MarshalResponse encodes a value as JSON for NATS responses.
func MarshalResponse(v any) ([]byte, error) {
	return json.Marshal(v)
}

// MarshalError encodes an error message as a JSON ErrorResponse.
func MarshalError(errMsg string) []byte {
	data, _ := json.Marshal(model.ErrorResponse{Error: errMsg})
	return data
}

// ReplyJSON sends a JSON-encoded response to a NATS message.
func ReplyJSON(msg *nats.Msg, v any) {
	data, err := MarshalResponse(v)
	if err != nil {
		ReplyError(msg, "marshal error: "+err.Error())
		return
	}
	if err := msg.Respond(data); err != nil {
		slog.Error("reply failed", "error", err)
	}
}

// ReplyError sends a JSON-encoded error response to a NATS message.
func ReplyError(msg *nats.Msg, errMsg string) {
	if err := msg.Respond(MarshalError(errMsg)); err != nil {
		slog.Error("error reply failed", "error", err)
	}
}

// TryParseError returns the ErrorResponse iff data decodes cleanly with a non-empty Error.
func TryParseError(data []byte) (model.ErrorResponse, bool) {
	var r model.ErrorResponse
	if err := json.Unmarshal(data, &r); err != nil || r.Error == "" {
		return model.ErrorResponse{}, false
	}
	return r, true
}
