package main

import (
	"fmt"
	"os"
	"strings"
)

// j1Source yields the current J1 token for the accessToken exchange. It is read
// fresh on each J1→J2 exchange so a rotated Kubernetes ServiceAccount token
// (kubelet rewrites the mounted file in place) is picked up without a restart.
type j1Source func() (string, error)

// staticJ1 returns a source that always yields tok. Used for the
// TRANSLATION_J1_TOKEN literal (local/dev) and in tests.
func staticJ1(tok string) j1Source {
	return func() (string, error) { return tok, nil }
}

// fileJ1 returns a source that reads and trims the token from path on every
// call. path is typically the projected ServiceAccount token mount
// (/var/run/secrets/kubernetes.io/serviceaccount/token). The token contents are
// never included in error messages.
func fileJ1(path string) j1Source {
	return func() (string, error) {
		// #nosec G304 -- path is an operator-configured token mount, not user input
		// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
		// nosemgrep: gosec.G304-1
		b, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read j1 token file %q: %w", path, err)
		}
		tok := strings.TrimSpace(string(b))
		if tok == "" {
			return "", fmt.Errorf("j1 token file %q is empty", path)
		}
		return tok, nil
	}
}

// newJ1Source selects the J1 source by precedence: the TRANSLATION_J1_TOKEN
// literal wins when set (local/dev), otherwise the file at
// TRANSLATION_J1_TOKEN_FILE (production: the ServiceAccount token). It errors
// when neither is configured. The literal is trimmed first so a whitespace-only
// value is treated as absent (matching fileJ1, which trims and rejects empty),
// rather than sending an invalid token.
func newJ1Source(literal, file string) (j1Source, error) {
	if lit := strings.TrimSpace(literal); lit != "" {
		return staticJ1(lit), nil
	}
	if file != "" {
		return fileJ1(file), nil
	}
	return nil, fmt.Errorf("one of TRANSLATION_J1_TOKEN or TRANSLATION_J1_TOKEN_FILE is required")
}
