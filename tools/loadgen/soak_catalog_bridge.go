package main

import (
	"time"

	soakcatalog "github.com/hmchangw/chat/tools/loadgen/internal/soak/catalog"
)

// These aliases are the compatibility boundary for the failure ledger and
// root adapter tests. The production runtime composes catalog directly; the
// aliases disappear when the failure runtime moves in the follow-up PR.
type soakCatalogAction = soakcatalog.Action

const (
	soakCatalogEdit         = soakcatalog.ActionEdit
	soakCatalogDelete       = soakcatalog.ActionDelete
	soakCatalogPin          = soakcatalog.ActionPin
	soakCatalogReaction     = soakcatalog.ActionReaction
	soakCatalogThreadParent = soakcatalog.ActionThreadParent
	soakCatalogThreadRead   = soakcatalog.ActionThreadRead
	soakCatalogReadReceipt  = soakcatalog.ActionReadReceipt
)

type soakClock = soakcatalog.TimeProvider
type soakCatalogCandidate = soakcatalog.Candidate
type soakCatalogMessage = soakcatalog.Message
type soakCatalog = soakcatalog.Catalog

func newSoakCatalog(
	perRoomCap int,
	globalCap int,
	persistGrace time.Duration,
	clock soakClock,
) *soakCatalog {
	return soakcatalog.New(perRoomCap, globalCap, persistGrace, clock)
}

func soakContentDigest(body string) string {
	return soakcatalog.ContentDigest(body)
}

func searchProbeTerm(content string) string {
	return soakcatalog.SearchTerm(content)
}
