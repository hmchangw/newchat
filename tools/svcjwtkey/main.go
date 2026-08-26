// Command svcjwtkey prints a fresh Ed25519 keypair in the base64 form
// SVCJWT_PRIVATE_KEY and SVCJWT_PUBLIC_KEY expect, so provisioning does not
// depend on an ad-hoc script.
//
// Usage: go run ./tools/svcjwtkey
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"os"
)

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run writes the keypair to w. Split from main so it is testable.
func run(w io.Writer) error {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return fmt.Errorf("generate ed25519 key: %w", err)
	}
	enc := base64.StdEncoding
	// The private line is the secret half: give it to the minting service only.
	if _, err := fmt.Fprintf(w, "SVCJWT_PRIVATE_KEY=%s\n", enc.EncodeToString(priv.Seed())); err != nil {
		return fmt.Errorf("write private key: %w", err)
	}
	if _, err := fmt.Fprintf(w, "SVCJWT_PUBLIC_KEY=%s\n", enc.EncodeToString(pub)); err != nil {
		return fmt.Errorf("write public key: %w", err)
	}
	return nil
}
