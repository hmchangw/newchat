package main

import (
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// newOutboxPublisher builds the callback pkg/outbox.Publish drives.
//
// JetStream, and blocking on the PubAck, is the whole point. This subject is
// the durable OUTBOX that outbox-worker drains, and the handler deletes the
// local subscription before it publishes — so a publish reported as successful
// but never persisted strands the remote membership with nothing left to
// retry, which is precisely what the OUTBOX exists to prevent. A core-NATS
// publish returns nil once the bytes reach the local socket buffer and would
// report exactly that false success. The dedup id rides as the Nats-Msg-Id so a
// redelivered removal collapses onto the existing entry.
func newOutboxPublisher(js natsutil.JetStreamMsgPublisher) outboxPublisher {
	return natsutil.JetStreamPublishFunc(js, natsmetrics.Publisher{})
}
