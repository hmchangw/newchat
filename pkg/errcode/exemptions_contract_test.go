package errcode

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
// the set of exempt files is pinned here: adding one turns this test red from a
// package the author did not touch, and going green requires editing the list
// in the same commit — putting the justification in front of a reviewer instead
// of letting it pass CI unread.
//
// This checks the SET, not the reasons. A wrong justification still merges if
// someone updates the list; what it removes is doing so silently.
var allowedParseExemptions = map[string]string{
	"admin-service/client_update.go": "decides nothing: the caller already classified by HTTP status, and this only lifts display text, so no refusal can reach a success path",
}

const exemptionMarker = "nosemgrep: remote-envelope-must-use-fromreply"

func TestParseExemptions_MatchThePinnedSet(t *testing.T) {
	root := repoRootFromErrcode(t)
	found := scanForExemptions(t, root)

	for _, path := range found {
		if _, ok := allowedParseExemptions[path]; !ok {
			t.Errorf(`%s has a new errcode.Parse exemption.

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
			"%s is listed as an exempt site (%s) but no longer carries the marker — "+
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

// scanForExemptions returns the repo-relative path of every non-test .go file
// carrying the nosemgrep marker, sorted.
func scanForExemptions(t *testing.T, root string) []string {
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

	var found []string
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
			return fmt.Errorf("relativise %q while scanning for exemptions: %w", path, relErr)
		}
		rel = filepath.ToSlash(rel)
		body, readErr := fs.ReadFile(rootFS, rel)
		if readErr != nil {
			return fmt.Errorf("read %q while scanning for exemptions: %w", rel, readErr)
		}
		if !strings.Contains(string(body), exemptionMarker) {
			return nil
		}
		found = append(found, rel)
		return nil
	})
	require.NoError(t, err)
	sort.Strings(found)
	return found
}
