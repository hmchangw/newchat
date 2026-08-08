package mongorepo

import "go.mongodb.org/mongo-driver/v2/mongo/readpref"

// Option customizes repo construction.
type Option func(*settings)

type settings struct {
	readPref *readpref.ReadPref
}

// WithReadPreference routes each repo's staleness-tolerant reads to rp; authz,
// dedup, read-after-write reads, and writes keep the default. Nil is a no-op.
func WithReadPreference(rp *readpref.ReadPref) Option {
	return func(s *settings) { s.readPref = rp }
}

func applyOptions(opts []Option) settings {
	var s settings
	for _, o := range opts {
		o(&s)
	}
	return s
}
