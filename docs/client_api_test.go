package docs

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func readClientAPIDoc(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(content)
}

// AC-5.1: the canonical client API documents the complete settings.get contract.
func TestClientAPI_AC_5_1_DocumentsSettingsGet(t *testing.T) {
	canonical := readClientAPIDoc(t, "client-api.md")

	require.Contains(t, canonical, "#### settings.get")
	require.Contains(t, canonical, "chat.user.{account}.request.user.{siteID}.settings.get")
	require.Contains(t, canonical, "##### UserSettingsData")
	require.Contains(t, canonical, "##### ChannelSectionSettings")
}

// AC-5.2: canonical and derived views both document settings.set with a success example.
func TestClientAPI_AC_5_2_DocumentsSettingsSet(t *testing.T) {
	canonical := readClientAPIDoc(t, "client-api.md")
	derived := readClientAPIDoc(t, "client-api/request-reply.md")

	require.Contains(t, canonical, "#### settings.set")
	require.Contains(t, canonical, "chat.user.{account}.request.user.{siteID}.settings.set")
	require.Contains(t, canonical, `"version": 8`)
	require.Contains(t, derived, "### settings.set")
	require.Contains(t, derived, `"version": 8`)
}

// AC-5.3: documentation defines the compare-and-set conflict retry flow.
func TestClientAPI_AC_5_3_DocumentsOptimisticLockRetry(t *testing.T) {
	canonical := readClientAPIDoc(t, "client-api.md")
	derived := readClientAPIDoc(t, "client-api/request-reply.md")

	require.Contains(t, canonical, "##### Optimistic-lock retry")
	require.Contains(t, canonical, "On `conflict`, call `settings.get` again")
	require.Contains(t, derived, "On `conflict`, clients must read the latest document")
}
