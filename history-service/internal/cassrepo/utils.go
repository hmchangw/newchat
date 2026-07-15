package cassrepo

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"reflect"
	"sync"

	"github.com/gocql/gocql"
)

// Cursor wraps Cassandra's PageState as a base64-encoded pagination token.
type Cursor struct {
	state []byte
}

// NewCursor decodes a base64-encoded cursor; empty string returns a first-page cursor.
func NewCursor(encoded string) (*Cursor, error) {
	if encoded == "" {
		return &Cursor{}, nil
	}
	if len(encoded) > base64.StdEncoding.EncodedLen(maxCursorBytes) {
		return nil, fmt.Errorf("decode cursor: encoded length %d exceeds maximum of %d",
			len(encoded), base64.StdEncoding.EncodedLen(maxCursorBytes))
	}
	state, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode cursor: %w", err)
	}
	return &Cursor{state: state}, nil
}

// Encode returns the base64 cursor string, or "" when there are no more pages.
func (c *Cursor) Encode() string {
	if len(c.state) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(c.state)
}

func (c *Cursor) Raw() []byte { return c.state }

type Page[T any] struct {
	Data       []T    `json:"data"`
	NextCursor string `json:"nextCursor,omitempty"`
	HasNext    bool   `json:"hasNext"`
}

type PageRequest struct {
	Cursor   *Cursor
	PageSize int
}

const (
	defaultCassPageSize = 50
	maxPageSize         = 100
)

// maxCursorBytes caps decoded page-state size; 512 is generous vs. real tokens (10–100 B) yet blocks pathological allocations.
const maxCursorBytes = 512

// ParsePageRequest validates and normalizes cursor and pageSize (default 50, max 100).
func ParsePageRequest(cursorStr string, pageSize int) (PageRequest, error) {
	cursor, err := NewCursor(cursorStr)
	if err != nil {
		return PageRequest{}, fmt.Errorf("parse page request cursor: %w", err)
	}
	if pageSize <= 0 {
		pageSize = defaultCassPageSize
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	return PageRequest{Cursor: cursor, PageSize: pageSize}, nil
}

type QueryBuilder struct {
	query    *gocql.Query
	cursor   *Cursor
	pageSize int
}

func NewQueryBuilder(q *gocql.Query) *QueryBuilder {
	return &QueryBuilder{query: q, pageSize: defaultCassPageSize}
}

func (b *QueryBuilder) WithCursor(cursor *Cursor) *QueryBuilder {
	b.cursor = cursor
	return b
}

func (b *QueryBuilder) WithPageSize(size int) *QueryBuilder {
	b.pageSize = size
	return b
}

// cqlIndexCache memoizes the cql-tag → field-index mapping per struct type so
// structScan doesn't rebuild it for every scanned row. Keyed by reflect.Type,
// value is map[string]int.
var cqlIndexCache sync.Map

// cqlFieldIndex returns the cql-tag → field-index map for rt, building and
// caching it on first use. Fields without a cql tag (or tagged "-") are omitted.
func cqlFieldIndex(rt reflect.Type) map[string]int {
	if cached, ok := cqlIndexCache.Load(rt); ok {
		return cached.(map[string]int)
	}
	m := make(map[string]int, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		tag := rt.Field(i).Tag.Get("cql")
		if tag == "" || tag == "-" {
			continue
		}
		m[tag] = i
	}
	actual, _ := cqlIndexCache.LoadOrStore(rt, m)
	return actual.(map[string]int)
}

// buildScanValues maps colNames to addressable field pointers via cql struct tags, returning the slice for iter.Scan.
// Separated from structScan so the column-matching logic is unit-testable without a live gocql iterator.
func buildScanValues(dest any, colNames []string) (values []any, missingCol string, ok bool) {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.Elem().Kind() != reflect.Struct {
		return nil, "", false
	}
	rv = rv.Elem()
	idxByTag := cqlFieldIndex(rv.Type())

	vals := make([]any, len(colNames))
	for i, name := range colNames {
		idx, found := idxByTag[name]
		if !found {
			return nil, name, false
		}
		vals[i] = rv.Field(idx).Addr().Interface()
	}
	return vals, "", true
}

// structScan scans one row into dest via positional iter.Scan using cql struct tags.
// Positional scan is used instead of MapScan because MapScan's RowData() panics on MAP<frozen<UDT>,frozen<UDT>> keys.
func structScan(iter *gocql.Iter, dest any) (bool, error) {
	// Validate dest shape before touching the iterator (iter may be nil in tests).
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.Elem().Kind() != reflect.Struct {
		return false, nil
	}

	cols := iter.Columns()
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.Name
	}

	values, missingCol, ok := buildScanValues(dest, colNames)
	if !ok {
		err := fmt.Errorf("structScan: unmapped column %q for type %T", missingCol, dest)
		slog.Warn("structScan: unmapped column", "column", missingCol, "type", fmt.Sprintf("%T", dest))
		return false, err
	}
	return iter.Scan(values...), nil
}

// Fetch executes the paged query, calls scan with the iterator, and returns the encoded next-page cursor.
func (b *QueryBuilder) Fetch(scan func(iter *gocql.Iter)) (string, error) {
	if b.query == nil {
		return "", fmt.Errorf("execute paged query: nil query")
	}
	q := b.query.PageSize(b.pageSize)
	if b.cursor != nil {
		q = q.PageState(b.cursor.Raw())
	}

	iter := q.Iter()
	scan(iter)

	nextCursor := (&Cursor{state: iter.PageState()}).Encode()
	if err := iter.Close(); err != nil {
		return "", fmt.Errorf("close cassandra iterator: %w", err)
	}
	return nextCursor, nil
}
