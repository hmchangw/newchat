package natsmetrics

import (
	"bufio"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEveryRPCMethodIsValid pins the vocabulary as the closed set Valid()
// reports. A constant that fails here is declared but unusable: addRPCRoute
// degrades an invalid method to MethodOther, so the route would silently
// record under the fallback instead of its own name.
func TestEveryRPCMethodIsValid(t *testing.T) {
	for _, m := range rpcMethods {
		assert.True(t, m.Valid(), "method %q is declared but not Valid()", m)
	}
}

// TestMethodOtherIsNotValid keeps the fallback outside the vocabulary. _OTHER
// is what registration degrades to when a caller passes something unusable;
// if it were Valid() a route could claim it deliberately and the fallback
// would stop meaning "this should never happen".
func TestMethodOtherIsNotValid(t *testing.T) {
	assert.False(t, MethodOther.Valid(),
		"MethodOther must stay outside the vocabulary so no route can claim it")
}

// TestRPCMethodValuesAreUniqueAndWellFormed guards the label itself: a
// duplicate value silently merges two routes into one time series, and a value
// outside snake_case breaks the naming rule the vocabulary documents.
//
// rpcmethodgen rejects both at generation time, but this is the assertion that
// holds if the generated file is ever edited by hand or produced by a future
// generator, so it stays here rather than living only in the tool's tests.
func TestRPCMethodValuesAreUniqueAndWellFormed(t *testing.T) {
	snakeCase := regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)
	seen := make(map[RPCMethod]struct{}, len(rpcMethods))

	for _, m := range rpcMethods {
		_, dup := seen[m]
		require.False(t, dup, "duplicate RPCMethod value %q", m)
		seen[m] = struct{}{}
		assert.Regexp(t, snakeCase, string(m), "method %q is not verb-first snake_case", m)
	}
}

// TestGeneratedVocabularyMatchesItsSourceTable catches the one mistake the
// generator cannot: editing rpcmethods.tsv and not running `make generate`, or
// editing rpcmethod_gen.go by hand. It compares label values only — a constant
// missing from the generated file fails to compile at its call site, so the
// compiler already covers that half.
func TestGeneratedVocabularyMatchesItsSourceTable(t *testing.T) {
	table := readSourceTable(t)

	generated := make(map[RPCMethod]struct{}, len(rpcMethods))
	for _, m := range rpcMethods {
		generated[m] = struct{}{}
	}

	assert.Equal(t, len(table), len(generated),
		"rpcmethods.tsv and rpcmethod_gen.go disagree on how many methods exist; run `make generate`")

	for _, value := range table {
		assert.Contains(t, generated, value,
			"rpcmethods.tsv declares %q but the generated vocabulary does not; run `make generate`", value)
	}
}

// readSourceTable returns the label values in rpcmethods.tsv. It re-reads the
// source rather than importing the generator, which is package main — and that
// independence is the point: a parser shared with the tool could not catch the
// tool having run against a stale table.
func readSourceTable(t *testing.T) []RPCMethod {
	t.Helper()

	f, err := os.Open("rpcmethods.tsv")
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	var values []RPCMethod
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		require.GreaterOrEqual(t, len(fields), 2, "malformed row: %s", line)
		values = append(values, RPCMethod(strings.TrimSpace(fields[1])))
	}
	require.NoError(t, scanner.Err())
	require.NotEmpty(t, values, "read no rows from rpcmethods.tsv; the test is broken, not the table")

	return values
}
