package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test exists because a soak RPC can be perfectly well-formed on the wire
// and still be rejected by every handler that receives it. `natsrouter.Register`
// decodes a body; `RegisterNoBody` does not. A call site that omits Body against
// a Register-ed subject sends `null`, which unmarshals cleanly into the zero
// value and is then refused by the handler's own validation — so the lane fails
// 100% of the time while every wire-shape test still passes. Comparing the call
// sites against the real registrations is the only check that catches it.

var (
	soakRegistrationPattern = regexp.MustCompile(
		`natsrouter\.(Register|RegisterNoBody)\(\s*\w+\s*,\s*subject\.(\w+)Pattern\(`,
	)
	soakRequestLiteralPattern = regexp.MustCompile(`soakRPCRequest\{`)
	soakRequestSubjectPattern = regexp.MustCompile(`Subject:\s*subject\.(\w+)\(`)
	soakRequestBodyPattern    = regexp.MustCompile(`\bBody:`)
)

// repoRoot is two levels up from tools/loadgen. Resolved rather than assumed so
// the test fails loudly if the package is ever moved.
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(root, "go.mod"),
		"expected the repo root two levels above tools/loadgen")
	return root
}

// handlerBodyRequirements maps a subject builder name to whether the handler
// registered for its pattern decodes a request body.
func handlerBodyRequirements(t *testing.T, root string) map[string]bool {
	t.Helper()
	skipDirs := map[string]bool{
		".git": true, "node_modules": true, "chat-frontend": true,
		"admin-frontend": true, "docs": true,
	}
	requirements := make(map[string]bool)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
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
		source, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		for _, match := range soakRegistrationPattern.FindAllStringSubmatch(string(source), -1) {
			// A subject registered both ways anywhere in the repo is treated as
			// body-required, the stricter reading.
			requirements[match[2]] = requirements[match[2]] || match[1] == "Register"
		}
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, requirements,
		"found no natsrouter registrations; the scan is broken, not the code")
	return requirements
}

type soakRequestCallSite struct {
	file    string
	line    int
	builder string
	hasBody bool
}

// soakRequestCallSites extracts every soakRPCRequest literal in the soak
// sources, brace-matched so a nested literal cannot truncate the scan.
func soakRequestCallSites(t *testing.T) []soakRequestCallSite {
	t.Helper()
	files, err := filepath.Glob("soak_*.go")
	require.NoError(t, err)

	var sites []soakRequestCallSite
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		source, readErr := os.ReadFile(file)
		require.NoError(t, readErr)
		text := string(source)
		for _, match := range soakRequestLiteralPattern.FindAllStringIndex(text, -1) {
			literal, ok := matchSoakBraces(text, match[1])
			if !ok {
				continue
			}
			subjectMatch := soakRequestSubjectPattern.FindStringSubmatch(literal)
			if subjectMatch == nil {
				continue
			}
			sites = append(sites, soakRequestCallSite{
				file:    file,
				line:    strings.Count(text[:match[0]], "\n") + 1,
				builder: subjectMatch[1],
				hasBody: soakRequestBodyPattern.MatchString(literal),
			})
		}
	}
	require.NotEmpty(t, sites, "found no soakRPCRequest call sites; the scan is broken")
	return sites
}

// matchSoakBraces returns the literal body starting just past its opening brace.
func matchSoakBraces(text string, start int) (string, bool) {
	depth := 1
	for i := start; i < len(text); i++ {
		switch text[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return text[start:i], true
			}
		}
	}
	return "", false
}

func TestSoakRPCCallSites_CarryABodyWhenTheHandlerDecodesOne(t *testing.T) {
	requirements := handlerBodyRequirements(t, repoRoot(t))
	sites := soakRequestCallSites(t)

	checked := 0
	for _, site := range sites {
		requiresBody, known := requirements[site.builder]
		if !known {
			// Server-to-server and presence subjects are not natsrouter-served;
			// they have no registration to compare against.
			continue
		}
		checked++
		if !requiresBody {
			continue
		}
		assert.True(t, site.hasBody,
			"%s:%d sends no body to subject.%s, whose handler is registered with "+
				"natsrouter.Register and decodes one; the request would arrive as "+
				"null and be rejected by the handler's validation",
			site.file, site.line, site.builder)
	}
	assert.Positive(t, checked,
		"no soak call site matched a known registration; the mapping is broken")
}
