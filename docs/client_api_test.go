package docs

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func readCanonicalClientAPI(t *testing.T) string {
	t.Helper()
	content, err := os.ReadFile("client-api.md")
	require.NoError(t, err)
	return string(content)
}

func readRequestReplyClientAPI(t *testing.T) string {
	t.Helper()
	content, err := os.ReadFile("client-api/request-reply.md")
	require.NoError(t, err)
	return string(content)
}

// sectionBetween extracts text starting at startMarker up to the next section
// heading of the same or higher level (### or ####). If end markers are absent,
// the remainder of the document is returned.
func sectionBetween(doc, startMarker string, endMarkers ...string) string {
	start := strings.Index(doc, startMarker)
	if start < 0 {
		return ""
	}
	rest := doc[start:]
	// Skip the start line itself so we search only for subsequent headings.
	nl := strings.Index(rest, "\n")
	if nl < 0 {
		return rest
	}
	body := rest[nl+1:]
	end := len(body)
	for _, m := range endMarkers {
		if i := strings.Index(body, m); i >= 0 && i < end {
			end = i
		}
	}
	return rest[:nl+1+end]
}

// AC-5.1: the canonical and derived client APIs document the complete settings.get contract.
func TestClientAPI_AC_5_1_DocumentsSettingsGet(t *testing.T) {
	canonical := readCanonicalClientAPI(t)
	derived := readRequestReplyClientAPI(t)

	canonGet := sectionBetween(canonical, "#### settings.get", "#### settings.set", "#### status.getByName")
	derivedGet := sectionBetween(derived, "### settings.get", "### settings.set", "### status.getByName")

	require.NotEmpty(t, canonGet, "canonical settings.get section missing")
	require.Contains(t, canonGet, "chat.user.{account}.request.user.{siteID}.settings.get")
	require.Contains(t, canonGet, "##### UserSettingsData")
	require.Contains(t, canonGet, "##### ChannelSectionSettings")

	require.NotEmpty(t, derivedGet, "derived settings.get section missing")
	require.Contains(t, derivedGet, "chat.user.{account}.request.user.{siteID}.settings.get")
	require.Contains(t, derivedGet, "UserSettingsData")
}

// AC-5.2: canonical and derived views both document the complete settings.set contract.
func TestClientAPI_AC_5_2_DocumentsSettingsSet(t *testing.T) {
	canonical := readCanonicalClientAPI(t)
	derived := readRequestReplyClientAPI(t)

	canonSet := sectionBetween(canonical, "#### settings.set", "#### status.getByName")
	derivedSet := sectionBetween(derived, "### settings.set", "### status.getByName")

	require.NotEmpty(t, canonSet, "canonical settings.set section missing")
	require.Contains(t, canonSet, "chat.user.{account}.request.user.{siteID}.settings.set")
	require.Contains(t, canonSet, "ifVersion")
	require.Contains(t, canonSet, `"version": 8`)
	require.Contains(t, canonSet, "`conflict`")
	require.Contains(t, canonSet, "`bad_request`")

	require.NotEmpty(t, derivedSet, "derived settings.set section missing")
	require.Contains(t, derivedSet, "chat.user.{account}.request.user.{siteID}.settings.set")
	require.Contains(t, derivedSet, "ifVersion")
	require.Contains(t, derivedSet, `"version": 8`)
	require.Contains(t, derivedSet, "conflict")
	require.Contains(t, derivedSet, "bad_request")
}

// AC-5.3: documentation defines the full compare-and-set conflict retry flow.
func TestClientAPI_AC_5_3_DocumentsOptimisticLockRetry(t *testing.T) {
	canonical := readCanonicalClientAPI(t)
	derived := readRequestReplyClientAPI(t)

	canonSet := sectionBetween(canonical, "#### settings.set", "#### status.getByName")
	derivedSet := sectionBetween(derived, "### settings.set", "### status.getByName")

	require.Contains(t, canonSet, "##### Optimistic-lock retry")
	require.Contains(t, canonSet, "settings.get")
	require.Contains(t, canonSet, "ifVersion")
	require.Contains(t, canonSet, "On `conflict`, call `settings.get` again")

	require.Contains(t, derivedSet, "On `conflict`, clients must read the latest document")
	require.Contains(t, derivedSet, "ifVersion")
}
