package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/poolartifact"
)

// writePoolArtifact projects the seeded users' accounts (fixture order
// preserved) into the shared pool artifact clientsim consumes.
func writePoolArtifact(path, runID, siteID, digest string, users []model.User) error {
	if len(users) == 0 {
		return errors.New("write pool artifact: no seeded users")
	}
	accounts := make([]string, len(users))
	for i := range users {
		accounts[i] = users[i].Account
	}
	return poolartifact.Write(path, &poolartifact.Artifact{
		RunID: runID, SiteID: siteID, ConfigDigest: digest, Accounts: accounts,
	})
}

// seedConfigDigest fingerprints the fixture inputs so a pool artifact can be
// matched to the seed run that produced it.
func seedConfigDigest(presetName string, seed int64, users int) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s|%d|%d", presetName, seed, users))
	return hex.EncodeToString(sum[:8])
}
