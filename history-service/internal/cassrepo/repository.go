package cassrepo

import (
	"github.com/gocql/gocql"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

// tracerName identifies this package's spans in the global tracer provider.
const tracerName = "github.com/hmchangw/chat/history-service/internal/cassrepo"

// Repository wraps a Cassandra session with the bucket sizer + read-walk
// configuration shared by all queries against bucketed message tables, plus
// an optional at-rest Cipher for encrypted message bodies.
type Repository struct {
	session    *gocql.Session
	bucket     msgbucket.Sizer
	maxBuckets int
	cipher     atrest.Cipher // nil when ATREST_ENABLED=false
	tracer     trace.Tracer
}

// NewRepository wires a session, bucket sizer, max-walk depth, and (optional)
// at-rest Cipher. maxBuckets caps how far a paginated read walks through empty
// buckets before returning a non-terminal cursor. cipher may be nil; when nil
// the read path treats encountered enc_payload rows as a configuration error
// and the write path uses legacy plaintext columns.
func NewRepository(session *gocql.Session, bucket msgbucket.Sizer, maxBuckets int, cipher atrest.Cipher) *Repository {
	return &Repository{
		session:    session,
		bucket:     bucket,
		maxBuckets: maxBuckets,
		cipher:     cipher,
		tracer:     otel.Tracer(tracerName),
	}
}
