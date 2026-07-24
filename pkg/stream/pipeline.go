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

// Wiring is everything a fan-out worker needs to bind to a pipeline.
type Wiring struct {
	CanonicalStream   Config // MESSAGES_CANONICAL or BOT_MESSAGES_CANONICAL
	CanonicalCreated  string // .created leaf — notification-worker filter
	CanonicalWildcard string // .> wildcard — broadcast-worker filter
	PushStream        Config // PUSH_NOTIFICATION or BOT_PUSH_NOTIFICATION
	PushSendSubject   string // .send leaf — notification-worker publishes here
	PushInputWildcard string // .> wildcard — push-notification-service filter, also the push-stream binding
}

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
	}
}
