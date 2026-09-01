package stream

import (
	"fmt"

	"github.com/hmchangw/chat/pkg/subject"
)

// Pipeline selects which canonical/push stream pair a worker binds to.
type Pipeline string

const (
	PipelineUser Pipeline = "user"
	PipelineBot  Pipeline = "bot"
)

// UnmarshalText validates MODE at env-parse time so callers don't re-check.
func (p *Pipeline) UnmarshalText(b []byte) error {
	switch v := Pipeline(b); v {
	case PipelineUser, PipelineBot:
		*p = v
		return nil
	default:
		return fmt.Errorf("invalid pipeline %q; must be one of: user, bot", string(b))
	}
}

// ConsumerName prefixes bot durables with "bot-" so metrics/logs distinguish
// the two deployments; user mode keeps base unchanged.
func (p Pipeline) ConsumerName(base string) string {
	if p == PipelineBot {
		return "bot-" + base
	}
	return base
}

// FailoverConsumerName is the buddy-lane durable for a pipeline: the home-lane
// name plus a suffix. Distinct from the home lane's so the two keep independent
// cursors — on a single-server dev NATS both lanes live on one server, and a
// shared durable would have them clobber each other. Shared rather than
// re-derived per service so two lanes on one buddy cluster cannot drift apart.
func (p Pipeline) FailoverConsumerName(base string) string {
	return p.ConsumerName(base) + "-failover"
}

// Wiring is everything a fan-out worker needs to bind to a pipeline.
type Wiring struct {
	CanonicalStream   Config // MESSAGES-CANONICAL or BOT-MESSAGES-CANONICAL
	CanonicalCreated  string // .created leaf — notification-worker filter
	CanonicalWildcard string // .> wildcard — broadcast-worker filter
	PushStream        Config // PUSH-NOTIFICATION or BOT-PUSH-NOTIFICATION
	PushSendSubject   string // .send leaf — notification-worker publishes here
	PushInputWildcard string // .> wildcard — push-notification-service filter, also the push-stream binding

	// Failover lane, populated for the user pipeline only. Zero for
	// PipelineBot — bots are not displaced users. Guard reads with HasFailover.
	CanonicalFailoverStream   Config
	CanonicalFailoverCreated  string
	CanonicalFailoverWildcard string
	PushFailoverStream        Config
	PushFailoverSendSubject   string
	PushFailoverInputWildcard string
}

// HasFailover reports whether this pipeline has a standby failover lane, so a
// service can skip binding one without testing the pipeline mode itself.
func (w *Wiring) HasFailover() bool { return w.CanonicalFailoverStream.Name != "" }

// Resolve returns the full wiring for a pipeline at a site.
func Resolve(p Pipeline, siteID string) Wiring {
	if p == PipelineBot {
		return Wiring{
			CanonicalStream:   BotMessagesCanonical(siteID),
			CanonicalCreated:  subject.BotCanonicalCreated(siteID),
			CanonicalWildcard: subject.BotCanonicalWildcard(siteID),
			PushStream:        BotPushNotification(siteID),
			PushSendSubject:   subject.BotPushNotification(siteID, "send"),
			PushInputWildcard: subject.BotPushNotificationWildcard(siteID),
		}
	}
	return Wiring{
		CanonicalStream:   MessagesCanonical(siteID),
		CanonicalCreated:  subject.MsgCanonicalCreated(siteID),
		CanonicalWildcard: subject.MsgCanonicalWildcard(siteID),
		PushStream:        PushNotification(siteID),
		PushSendSubject:   subject.PushNotification(siteID),
		PushInputWildcard: subject.PushNotificationFilter(siteID),

		CanonicalFailoverStream:   MessagesCanonicalFailover(siteID),
		CanonicalFailoverCreated:  subject.FailoverMsgCanonicalCreated(siteID),
		CanonicalFailoverWildcard: subject.FailoverMsgCanonicalWildcard(siteID),
		PushFailoverStream:        PushNotificationFailover(siteID),
		PushFailoverSendSubject:   subject.FailoverPushNotification(siteID),
		PushFailoverInputWildcard: subject.FailoverPushNotificationFilter(siteID),
	}
}
