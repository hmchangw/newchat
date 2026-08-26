package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/svcjwt"
)

// parseEnv turns the "K=V" lines run writes into a map.
func parseEnv(t *testing.T, s string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
		require.True(t, ok, "line %q is not K=V", line)
		out[k] = v
	}
	return out
}

func TestRun_EmitsBothKeys(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, run(&buf))

	env := parseEnv(t, buf.String())
	assert.NotEmpty(t, env["SVCJWT_PRIVATE_KEY"])
	assert.NotEmpty(t, env["SVCJWT_PUBLIC_KEY"])
	assert.NotEqual(t, env["SVCJWT_PRIVATE_KEY"], env["SVCJWT_PUBLIC_KEY"])
}

// TestRun_KeysWorkTogether is the point of the tool: the printed pair must
// actually sign and verify.
func TestRun_KeysWorkTogether(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, run(&buf))
	env := parseEnv(t, buf.String())

	signer, err := svcjwt.NewSigner(env["SVCJWT_PRIVATE_KEY"], "admin-service")
	require.NoError(t, err)
	verifier, err := svcjwt.NewVerifier(env["SVCJWT_PUBLIC_KEY"], "admin-service", "client-update-service")
	require.NoError(t, err)

	token, _, err := signer.Sign("svc-updater", "client-update-service", time.Hour)
	require.NoError(t, err)
	claims, err := verifier.Verify(token)
	require.NoError(t, err)
	assert.Equal(t, "svc-updater", claims.Subject)
}

func TestRun_KeysDifferEachInvocation(t *testing.T) {
	var a, b bytes.Buffer
	require.NoError(t, run(&a))
	require.NoError(t, run(&b))
	assert.NotEqual(t, a.String(), b.String())
}
