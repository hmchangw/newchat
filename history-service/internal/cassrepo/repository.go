package cassrepo

import (
	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

// Repository wraps a Cassandra session with the bucket sizer + read-walk
// configuration shared by all queries against bucketed message tables, plus
// an optional at-rest Cipher for encrypted message bodies.
type Repository struct {
	session             *gocql.Session
	bucket              msgbucket.Sizer
	maxBuckets          int
	previewLookbackRows int
	cipher              atrest.Cipher // nil when ATREST_ENABLED=false
}

// NewRepository wires a session, bucket sizer, read-walk depths, and
// (optional) at-rest Cipher. maxBuckets caps how far a paginated read walks
// through empty buckets before returning a non-terminal cursor.
// previewLookbackRows caps the TOTAL rows GetLastRoomMessage examines
// (MESSAGE_PREVIEW_LOOKBACK_ROWS, default 10): when the newest candidates are
// all deleted/system within the budget, the room shows no preview. cipher may
// be nil; when nil the read path treats encountered enc_payload rows as a
// configuration error and the write path uses legacy plaintext columns.
func NewRepository(session *gocql.Session, bucket msgbucket.Sizer, maxBuckets, previewLookbackRows int, cipher atrest.Cipher) *Repository {
	return &Repository{
		session:             session,
		bucket:              bucket,
		maxBuckets:          maxBuckets,
		previewLookbackRows: previewLookbackRows,
		cipher:              cipher,
	}
}
