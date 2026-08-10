package mongoutil

import (
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// WithReadPreference binds a client-level read preference on Connect. Nil is a
// no-op (default: primary). For a fixed secondaryPreferred client, use ConnectRead.
func WithReadPreference(rp *readpref.ReadPref) Option {
	return func(c *connectConfig) { c.readPref = rp }
}

// ParseReadPreference maps a config string to a read preference (case-insensitive,
// trimmed). Empty defaults to primary so an unset env preserves the driver default.
func ParseReadPreference(s string) (*readpref.ReadPref, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return readpref.Primary(), nil
	}
	mode, err := readpref.ModeFromString(s)
	if err != nil {
		return nil, fmt.Errorf("parse read preference %q: %w", s, err)
	}
	rp, err := readpref.New(mode)
	if err != nil {
		return nil, fmt.Errorf("build read preference %q: %w", s, err)
	}
	return rp, nil
}
