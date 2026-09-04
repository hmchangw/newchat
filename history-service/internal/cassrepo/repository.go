package cassrepo

import (
	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/threadcount"
)

// defaultWalkFanout caps how many buckets a bucketed-table read fetches
// concurrently once the walk fans out past its start bucket. It trades a bounded
// amount of speculative over-read for turning a long serial run of sparse/empty
// buckets into a few concurrent round-trips. It is kept at or below the per-host
// connection count in pkg/cassutil (default 8, CASSANDRA_NUMCONNS) so a single
// walk stays within one host's pool — if that connection count is lowered below
// this, revisit this value too.
const defaultWalkFanout = 8

// Repository wraps a Cassandra session with the bucket sizer + read-walk
// configuration shared by all queries against bucketed message tables, plus
// an optional at-rest Cipher for encrypted message bodies.
type Repository struct {
	session    *gocql.Session
	bucket     msgbucket.Sizer
	maxBuckets int
	walkFanout int
	cipher     atrest.Cipher // nil when ATREST_ENABLED=false
	// threadPolicy tunes where exact counting stops and how often an
	// approximate count is re-derived; tests override it.
	threadPolicy threadcount.Policy
}

// NewRepository wires a session, bucket sizer, max-walk depth, and (optional)
// at-rest Cipher. maxBuckets caps how far a paginated read walks through empty
// buckets before returning a non-terminal cursor. cipher may be nil; when nil
// the read path treats encountered enc_payload rows as a configuration error
// and the write path uses legacy plaintext columns.
func NewRepository(session *gocql.Session, bucket msgbucket.Sizer, maxBuckets int, cipher atrest.Cipher) *Repository {
	return &Repository{
		session:      session,
		bucket:       bucket,
		maxBuckets:   maxBuckets,
		walkFanout:   defaultWalkFanout,
		cipher:       cipher,
		threadPolicy: threadcount.DefaultPolicy(),
	}
}
