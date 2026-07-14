package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// InjectMode selects which subject the generator publishes onto.
type InjectMode string

const (
	InjectFrontdoor InjectMode = "frontdoor"
	InjectCanonical InjectMode = "canonical"
)

// ParseInjectMode converts a CLI flag value to an InjectMode.
func ParseInjectMode(s string) (InjectMode, error) {
	switch InjectMode(s) {
	case InjectFrontdoor, InjectCanonical:
		return InjectMode(s), nil
	default:
		return "", fmt.Errorf("unknown inject mode %q (want frontdoor|canonical)", s)
	}
}

// Publisher abstracts NATS publishing so tests can inject a recorder.
type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

// GeneratorConfig is the parameter bundle for a Generator.
// Preset is *Preset because the struct is large enough that gocritic's
// hugeParam rule would flag the embedded value.
type GeneratorConfig struct {
	Preset         *Preset
	Fixtures       Fixtures
	SiteID         string
	Rate           int
	Inject         InjectMode
	Publisher      Publisher
	Metrics        *Metrics
	Collector      *Collector
	WarmupDeadline time.Time
	// MaxInFlight caps concurrent publishes dispatched from the ticker.
	// Set to 0 to publish serially on the ticker goroutine (legacy behavior,
	// useful for bisection).
	MaxInFlight int
	// ParentsByRoom, when non-nil, switches the frontdoor path into thread-reply
	// mode: each send sets ThreadParentMessageID to a random seeded parent of the
	// target room. nil = plain sends. Keyed by room ID.
	ParentsByRoom map[string][]threadParent
}

// Generator is the open-loop publisher.
type Generator struct {
	cfg     GeneratorConfig
	rngMu   sync.Mutex
	rng     *rand.Rand
	maxBody string
}

// NewGenerator returns a Generator seeded from `seed`.
func NewGenerator(cfg *GeneratorConfig, seed int64) *Generator {
	max := cfg.Preset.ContentBytes.Max
	if max <= 0 {
		max = 1
	}
	return &Generator{
		cfg:     *cfg,
		rng:     rand.New(rand.NewSource(seed)),
		maxBody: strings.Repeat("x", max),
	}
}

// drainGracePeriod bounds how long Run waits for in-flight publishes
// to complete after ctx cancels.
const drainGracePeriod = 5 * time.Second

// Run publishes at the configured rate until ctx is cancelled. When
// MaxInFlight > 0 it drives a batched pacer (see runPaced); MaxInFlight == 0
// keeps the legacy serial-on-ticker path for bisection (see runSerial).
func (g *Generator) Run(ctx context.Context) error {
	if g.cfg.Rate <= 0 {
		return fmt.Errorf("rate must be > 0")
	}
	if g.cfg.MaxInFlight <= 0 {
		return g.runSerial(ctx)
	}
	return g.runPaced(ctx)
}

// runSerial is the legacy one-publish-per-tick path (MaxInFlight == 0), retained
// for bisection; it will not ramp past the single-ticker ceiling.
func (g *Generator) runSerial(ctx context.Context) error {
	serialDispatch(ctx, g.cfg.Rate, g.publishOne)
	return nil
}

// runPaced drives the batched pacer into a bounded worker pool so achieved RPS
// is not capped by single-ticker resolution. A full pool is recorded as
// "saturated" (raise MaxInFlight); events the pacer could not release on
// schedule as "underrun" (the load box could not keep up).
func (g *Generator) runPaced(ctx context.Context) error {
	pubErrs := g.cfg.Metrics.PublishErrors
	pacedDispatch(ctx, g.cfg.Rate, g.cfg.MaxInFlight,
		func(n int) {
			if n > 0 {
				pubErrs.WithLabelValues(g.cfg.Preset.Name, "underrun").Add(float64(n))
			}
		},
		func() { pubErrs.WithLabelValues(g.cfg.Preset.Name, "saturated").Inc() },
		g.publishOne)
	return nil
}

// intn returns rng.Intn(n) with mutex protection so publishOne is
// safe to call from multiple worker goroutines.
func (g *Generator) intn(n int) int {
	g.rngMu.Lock()
	defer g.rngMu.Unlock()
	return g.rng.Intn(n)
}

func (g *Generator) float64() float64 {
	g.rngMu.Lock()
	defer g.rngMu.Unlock()
	return g.rng.Float64()
}

func (g *Generator) publishOne(ctx context.Context) {
	if len(g.cfg.Fixtures.Subscriptions) == 0 {
		return
	}
	subIdx := g.intn(len(g.cfg.Fixtures.Subscriptions))
	sub := g.cfg.Fixtures.Subscriptions[subIdx]
	content := g.content()
	msgID := idgen.GenerateMessageID()
	publishTime := time.Now()

	var (
		subj  string
		data  []byte
		reqID string
		err   error
	)
	switch g.cfg.Inject {
	case InjectCanonical:
		now := time.Now().UTC()
		evt := model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: msgID, RoomID: sub.RoomID,
				UserID: sub.User.ID, UserAccount: sub.User.Account,
				Content: content, CreatedAt: now,
			},
			SiteID:    g.cfg.SiteID,
			Timestamp: now.UnixMilli(),
		}
		data, err = json.Marshal(evt)
		subj = subject.MsgCanonicalCreated(g.cfg.SiteID)
		g.cfg.Collector.RecordPublishBroadcastOnly(msgID, publishTime)
	default:
		reqID = idgen.GenerateRequestID()
		req := model.SendMessageRequest{ID: msgID, Content: content, RequestID: reqID}
		if g.cfg.ParentsByRoom != nil {
			parents := g.cfg.ParentsByRoom[sub.RoomID]
			if len(parents) == 0 {
				// Room has no seeded parents — cannot form a valid thread reply.
				return
			}
			req.ThreadParentMessageID = parents[g.intn(len(parents))].MessageID
		}
		data, err = json.Marshal(req)
		subj = subject.MsgSend(sub.User.Account, sub.RoomID, g.cfg.SiteID)
		g.cfg.Collector.RecordPublish(reqID, msgID, publishTime)
	}
	if err != nil {
		g.cfg.Metrics.PublishErrors.WithLabelValues(g.cfg.Preset.Name, "marshal").Inc()
		return
	}
	if perr := g.cfg.Publisher.Publish(ctx, subj, data); perr != nil {
		g.cfg.Collector.RecordPublishFailed(reqID, msgID)
		g.cfg.Metrics.PublishErrors.WithLabelValues(g.cfg.Preset.Name, "publish").Inc()
		return
	}
	phase := "measured"
	if publishTime.Before(g.cfg.WarmupDeadline) {
		phase = "warmup"
	}
	g.cfg.Metrics.Published.WithLabelValues(g.cfg.Preset.Name, phase).Inc()
}

func (g *Generator) content() string {
	r := g.cfg.Preset.ContentBytes
	size := r.Min
	if r.Max > r.Min {
		size = r.Min + g.intn(r.Max-r.Min+1)
	}
	if size <= 0 {
		size = 1
	}
	body := g.maxBody[:size]
	if g.cfg.Preset.MentionRate > 0 && g.float64() < g.cfg.Preset.MentionRate {
		target := g.intn(g.cfg.Preset.Users)
		body = fmt.Sprintf("@user-%d %s", target, body)
	}
	return body
}
