package errcode

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test exists because the semgrep rule cannot check the one thing that
// actually matters about an exemption: whether its reason is true.
//
// `nosemgrep` takes a free-text justification and suppresses the finding no
// matter what it says. Six sites were once exempt on the stated grounds that
// they "already handle !Code.Valid()", and five of the six had a live
// fail-open — Parse reports "not an envelope" for a payload it cannot decode,
// so it returns before any such guard runs. The rule's own message taught that
// bar, which is why every one of them was written the same wrong way.
//
// The message says something true now, but prose is what failed last time. So
// the set of files that reference errcode.Parse is pinned here: adding one
// turns this test red from a package the author did not touch, and going green
// requires editing the list in the same commit — putting the justification in
// front of a reviewer instead of letting it pass CI unread.
//
// It scans for the REFERENCE, not for the suppression. The first version of
// this test matched the literal `nosemgrep: remote-envelope-must-use-fromreply`
// and was bypassed two ways, both verified against semgrep before this rewrite:
// a bare `// nosemgrep` suppresses every rule, and a comma-separated list
// (`// nosemgrep: other-rule, remote-envelope-must-use-fromreply`) suppresses
// this one without the marker text ever appearing. A suppression has many
// spellings; the call it hides has one. Reported by coderabbitai on #477.
//
// This checks the SET, not the reasons. A wrong justification still merges if
// someone updates the list; what it removes is doing so silently.
var allowedParseExemptions = map[string]string{
	"admin-service/client_update.go": "decides nothing: the caller already classified by HTTP status, and this only lifts display text, so no refusal can reach a success path",
}

var (
	// parseReference matches errcode.Parse in any shape — call, assignment,
	// return, argument, or a function value — because the rule is about the
	// name being used at all.
	parseReference = regexp.MustCompile(`\berrcode\.Parse\b`)
	// dotImport would let a file call a bare `Parse(...)` that parseReference
	// cannot see. There are none today and this keeps it that way, so the scan
	// above stays sufficient rather than merely sufficient-for-now.
	dotImport = regexp.MustCompile(`(?m)^\s*(?:import\s+)?\.\s+"github\.com/hmchangw/chat/pkg/errcode"`)
)

func TestParseExemptions_MatchThePinnedSet(t *testing.T) {
	root := repoRootFromErrcode(t)
	found, dotImporters := scanForParseReferences(t, root)

	assert.Empty(t, dotImporters,
		"a dot-import of pkg/errcode lets a file call a bare Parse(...) that this scan cannot see; "+
			"import it normally so the reference stays greppable")

	for _, path := range found {
		if _, ok := allowedParseExemptions[path]; !ok {
			t.Errorf(`%s references errcode.Parse and is not a pinned exemption.

An exemption is only valid for a site that decides NOTHING from the reply. If it
decides success or failure it must use errcode.FromReply, because Parse answers
"does this fit this build's Error struct" — one added or retyped field makes it
report "not an envelope", and the reply falls past every !Code.Valid() guard
into whatever comes next. A positive success check afterwards is not a
substitute: `+"`status == \"ok\"`"+`, `+"`ack.OK`"+` and a non-nil collection each only hold
for a reply that omits that field.

If the exemption is genuinely justified, add it to allowedParseExemptions with
the reason. That edit is the point: it puts the reason in the diff.`, path)
		}
	}

	for path, reason := range allowedParseExemptions {
		assert.Contains(t, found, path,
			"%s is listed as an exempt site (%s) but no longer references errcode.Parse — "+
				"delete the entry so the list keeps describing the code", path, reason)
	}
}

func repoRootFromErrcode(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(root, "go.mod"),
		"expected the repo root two levels above pkg/errcode")
	return root
}

// scanForParseReferences returns the repo-relative path of every non-test .go
// file outside pkg/errcode that names errcode.Parse, plus any that dot-import
// the package. Both sorted.
func scanForParseReferences(t *testing.T, root string) (refs, dotImporters []string) {
	t.Helper()
	skipDirs := map[string]bool{
		".git": true, "node_modules": true, "chat-frontend": true,
		"admin-frontend": true, "docs": true, ".semgrep": true,
	}
	// Read through a root-scoped FS rather than the walked path: gosec's G122
	// flags the TOCTOU window between WalkDir yielding a path and opening it.
	// A suppression would be the easier answer and the wrong one to write in
	// this PR — the whole finding here is that justifications go unchecked.
	osRoot, err := os.OpenRoot(root)
	require.NoError(t, err)
	t.Cleanup(func() { _ = osRoot.Close() })
	rootFS := osRoot.FS()

	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("walk %q while scanning for exemptions: %w", path, err)
		}
		if entry.IsDir() {
			if skipDirs[entry.Name()] {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return fmt.Errorf("relativise %q while scanning for Parse references: %w", path, relErr)
		}
		rel = filepath.ToSlash(rel)
		// pkg/errcode owns Parse; the rule has never applied inside it.
		if strings.HasPrefix(rel, "pkg/errcode/") {
			return nil
		}
		body, readErr := fs.ReadFile(rootFS, rel)
		if readErr != nil {
			return fmt.Errorf("read %q while scanning for Parse references: %w", rel, readErr)
		}
		if parseReference.Match(body) {
			refs = append(refs, rel)
		}
		if dotImport.Match(body) {
			dotImporters = append(dotImporters, rel)
		}
		return nil
	})
	require.NoError(t, err)
	sort.Strings(refs)
	sort.Strings(dotImporters)
	return refs, dotImporters
}
