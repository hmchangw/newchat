package main

import (
	"math/rand"

	"github.com/nats-io/nats.go"

	soaksend "github.com/hmchangw/chat/tools/loadgen/internal/soak/send"
)

const (
	soakSendTopLevel    = soaksend.KindTopLevel
	soakSendThreadReply = soaksend.KindThreadReply
)

type soakSendConfig = soaksend.Config
type soakSendIDs = soaksend.IDs
type soakSendTarget = soaksend.Target
type soakPendingSend = soaksend.Pending

const (
	soakSendReplyAccepted  = soaksend.ReplyAccepted
	soakSendReplyRejected  = soaksend.ReplyRejected
	soakSendReplyMalformed = soaksend.ReplyMalformed
	soakSendReplyUnmatched = soaksend.ReplyUnmatched
)

type soakSendReplyResult = soaksend.ReplyResult
type soakSendLifecycle = soaksend.Lifecycle
type soakSenderOption = soaksend.Option
type soakSender = soaksend.Sender

type recipientExpectedRoute = soaksend.RecipientExpectedRoute

const (
	recipientExpectedRouteAny  = soaksend.RecipientRouteAny
	recipientExpectedRouteRoom = soaksend.RecipientRouteRoom
	recipientExpectedRouteUser = soaksend.RecipientRouteUser
)

type recipientSetSource = soaksend.RecipientSetSource

const (
	recipientSetSourceLegacy          = soaksend.RecipientSourceLegacy
	recipientSetSourceTopology        = soaksend.RecipientSourceTopology
	recipientSetSourceThreadFollowers = soaksend.RecipientSourceThreadFollowers
)

func withSoakSendLifecycle(
	lifecycle soakSendLifecycle,
	onError func(error),
) soakSenderOption {
	return soaksend.WithLifecycle(lifecycle, onError)
}

func newSoakSender(
	cfg soakSendConfig,
	catalog *soakCatalog,
	publisher Publisher,
	clock soakClock,
	rng *rand.Rand,
	ids *soakSendIDs,
	options ...soakSenderOption,
) *soakSender {
	return soaksend.New(cfg, catalog, publisher, clock, rng, ids, options...)
}

type soakResponseSubscription = soaksend.ResponseSubscription
type soakResponseSource = soaksend.ResponseSource
type natsSoakResponseSource = soaksend.NATSResponseSource

func newNATSSoakResponseSource(nc *nats.Conn) *natsSoakResponseSource {
	return soaksend.NewNATSResponseSource(nc)
}

func startSoakSendResponsesWithObserver(
	source soakResponseSource,
	sender *soakSender,
	observer func(soakSendReplyResult),
) (soakResponseSubscription, error) {
	return soaksend.StartResponsesWithObserver(source, sender, observer)
}
