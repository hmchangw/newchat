package main

import (
	"encoding/json"
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
)

// CDCEvent is the verifier's trigger: which document in which source
// collection changed. Payload content beyond the key is ignored — the check
// re-reads current state from the source (spec §6).
type CDCEvent struct {
	Collection string
	Op         string
	DocID      string
}

func decodeCDCEvent(data []byte) (CDCEvent, error) {
	var ev model.OplogEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		return CDCEvent{}, fmt.Errorf("unmarshal oplog event: %w", err)
	}
	if len(ev.DocumentKey) == 0 {
		return CDCEvent{}, fmt.Errorf("oplog event has no documentKey")
	}
	var key struct {
		ID any `json:"_id"`
	}
	if err := json.Unmarshal(ev.DocumentKey, &key); err != nil {
		return CDCEvent{}, fmt.Errorf("unmarshal documentKey: %w", err)
	}
	id, ok := key.ID.(string)
	if !ok {
		return CDCEvent{}, fmt.Errorf("documentKey._id is not a string: %T", key.ID)
	}
	return CDCEvent{Collection: ev.Collection, Op: ev.Op, DocID: id}, nil
}
