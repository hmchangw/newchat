package main

import (
	"fmt"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

type startKind int

const (
	startFromNow    startKind = iota // default cold start: stream from now
	startAfterToken                  // resume/seed via change-stream resume token
	startAtTime                      // resume/seed via operation time
)

// startPoint is the resolved per-collection stream start position.
type startPoint struct {
	Kind   startKind
	Token  bson.Raw // set when Kind == startAfterToken
	TimeMs int64    // set when Kind == startAtTime
}

// resolveStartPoint applies the spec §4.2 precedence: (1) env override (START_RESUME_TOKEN /
// START_AT_TIME, reseeds every restart), (2) persisted checkpoint, (3) cold start (START_MODE).
func resolveStartPoint(cfg *config, cp *Checkpoint) (startPoint, error) {
	// Tier 1: env overrides.
	if cfg.StartResumeToken != "" {
		tok, err := buildResumeToken(cfg.StartResumeToken)
		if err != nil {
			return startPoint{}, err
		}
		return startPoint{Kind: startAfterToken, Token: tok}, nil
	}
	if cfg.StartAtTime != "" {
		ms, err := parseTimeMs(cfg.StartAtTime)
		if err != nil {
			return startPoint{}, err
		}
		return startPoint{Kind: startAtTime, TimeMs: ms}, nil
	}

	// Tier 2: persisted checkpoint.
	if cp != nil {
		if len(cp.ResumeToken) > 0 {
			return startPoint{Kind: startAfterToken, Token: cp.ResumeToken}, nil
		}
		if cp.ClusterTime > 0 {
			return startPoint{Kind: startAtTime, TimeMs: cp.ClusterTime}, nil
		}
	}

	// Tier 3: cold start.
	switch cfg.StartMode {
	case "time":
		// parseConfig guarantees StartAtTime is set, but it was already handled
		// by the Tier-1 override branch above; reaching here means it was empty.
		return startPoint{}, fmt.Errorf("START_MODE=time requires START_AT_TIME")
	default: // "now"
		return startPoint{Kind: startFromNow}, nil
	}
}

// buildResumeToken wraps a _data hex string into a change-stream resume token
// document ({ _data: "..." }) as raw BSON.
func buildResumeToken(data string) (bson.Raw, error) {
	raw, err := bson.Marshal(bson.M{"_data": data})
	if err != nil {
		return nil, fmt.Errorf("build resume token: %w", err)
	}
	return raw, nil
}

// parseTimeMs accepts a unix-millis integer or an RFC3339 timestamp.
func parseTimeMs(s string) (int64, error) {
	if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
		return ms, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0, fmt.Errorf("parse START_AT_TIME %q (want unix-ms or RFC3339): %w", s, err)
	}
	return t.UTC().UnixMilli(), nil
}
