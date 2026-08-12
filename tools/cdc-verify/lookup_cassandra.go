package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/gocql/gocql"
)

type cassStore struct {
	session *gocql.Session
}

func newCassStore(session *gocql.Session) *cassStore { return &cassStore{session: session} }

var cqlIdent = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// buildSelect assembles a point-select; identifiers are re-checked here, values
// always bound. Empty cols selects the whole row (live-inspect view).
func buildSelect(table string, key map[string]any, cols []string) (string, []any, error) {
	if !cqlIdent.MatchString(table) {
		return "", nil, fmt.Errorf("invalid table identifier %q", table)
	}
	colList := "*"
	if len(cols) > 0 {
		for _, c := range cols {
			if !cqlIdent.MatchString(c) {
				return "", nil, fmt.Errorf("invalid column identifier %q", c)
			}
		}
		colList = strings.Join(cols, ", ")
	}
	var conds []string
	var args []any
	for _, k := range sortedMapKeys(key) {
		if !cqlIdent.MatchString(k) {
			return "", nil, fmt.Errorf("invalid key column identifier %q", k)
		}
		conds = append(conds, k+" = ?")
		args = append(args, key[k])
	}
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s", //nolint:gocritic
		colList, table, strings.Join(conds, " AND "))
	return q, args, nil
}

func (s *cassStore) SelectOne(ctx context.Context, table string, key map[string]any, cols []string) (result map[string]any, err error) {
	// A whole-row read must skip columns gocql can't scan — a struct-keyed map
	// (e.g. reactions map<frozen<udt>,...>) panics MapScan; resolve them out first.
	if len(cols) == 0 {
		cols, err = s.scannableColumns(ctx, table, key)
		if err != nil {
			return nil, err
		}
	}
	q, args, err := buildSelect(table, key, cols)
	if err != nil {
		return nil, fmt.Errorf("build select: %w", err)
	}
	// #nosec G201 -- identifiers validated against ^[a-zA-Z0-9_]+$ in buildSelect; values are bound parameters
	iter := s.session.Query(q, args...).WithContext(ctx).Iter()
	// Close always runs (even on panic unwind); a residual gocql type panic
	// degrades to an error rather than tearing down the request.
	defer func() {
		cerr := iter.Close()
		if r := recover(); r != nil {
			result, err = nil, fmt.Errorf("scan %s: %v", table, r)
			return
		}
		if cerr != nil {
			result, err = nil, fmt.Errorf("select from %s: %w", table, cerr)
		}
	}()
	var rows []map[string]any
	for {
		row := map[string]any{}
		if !iter.MapScan(row) {
			break
		}
		rows = append(rows, row)
		if len(rows) > 1 {
			break
		}
	}
	switch len(rows) {
	case 0:
		return nil, errNotFound
	case 1:
		return rows[0], nil
	default:
		return nil, errAmbiguous
	}
}

// scannableColumns lists a row's columns minus any gocql can't MapScan. It reads
// only column metadata, never a row, so it can't hit the panic it guards against.
func (s *cassStore) scannableColumns(ctx context.Context, table string, key map[string]any) ([]string, error) {
	q, args, err := buildSelect(table, key, nil)
	if err != nil {
		return nil, fmt.Errorf("build select: %w", err)
	}
	// #nosec G201 -- identifiers validated against ^[a-zA-Z0-9_]+$ in buildSelect; values are bound parameters
	iter := s.session.Query(q, args...).WithContext(ctx).Iter()
	var out []string
	for _, c := range iter.Columns() {
		if cqlScannable(c.TypeInfo) {
			out = append(out, c.Name)
		}
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("inspect columns of %s: %w", table, err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no scannable columns in %s", table)
	}
	return out, nil
}

// cqlScannable reports whether gocql can MapScan a column of this type: a map
// with a non-comparable key (frozen UDT/collection) can't be built (reflect.MapOf).
func cqlScannable(t gocql.TypeInfo) bool {
	ct, ok := t.(gocql.CollectionType)
	if !ok {
		return true
	}
	switch ct.Type() {
	case gocql.TypeMap:
		return cqlComparableKey(ct.Key) && cqlScannable(ct.Elem)
	case gocql.TypeList, gocql.TypeSet:
		return cqlScannable(ct.Elem)
	default:
		return true
	}
}

// cqlComparableKey reports whether a CQL type maps to a comparable Go type
// usable as a map key. UDTs and collections do not.
func cqlComparableKey(t gocql.TypeInfo) bool {
	switch t.Type() {
	case gocql.TypeUDT, gocql.TypeMap, gocql.TypeList, gocql.TypeSet, gocql.TypeTuple:
		return false
	default:
		return true
	}
}

var _ CassStore = (*cassStore)(nil)
