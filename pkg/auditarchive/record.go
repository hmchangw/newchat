package auditarchive

import (
	"encoding/json"
	"fmt"
)

// Record is one archived event: the canonical payload plus where it came from.
// Marshal is the canonical form that is hashed, encrypted into a segment
// frame, and verified later; encoding/json compacts Payload, so whitespace in
// the original bytes does not affect the hash.
type Record struct {
	Site    string          `json:"site"`
	Stream  string          `json:"stream"`
	Seq     uint64          `json:"seq"`
	Subject string          `json:"subject"`
	EventAt int64           `json:"eventAt"`
	Payload json.RawMessage `json:"payload"`
}

// Marshal returns the canonical JSON of the record.
func (r Record) Marshal() ([]byte, error) { //nolint:gocritic // hugeParam: value receiver is the package API; Record is read-only here
	b, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("marshal archive record: %w", err)
	}
	return b, nil
}
