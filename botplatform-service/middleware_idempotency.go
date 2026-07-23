package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

// sentinelClient is the narrow Valkey surface botIdempotency calls.
type sentinelClient interface {
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	Del(ctx context.Context, keys ...string) error
}

// timeProvider abstracts time.Now so tests can pin the 60s bucket.
type timeProvider interface {
	Now() time.Time
}

type realTime struct{}

func (realTime) Now() time.Time { return time.Now() }

// resourceIDFunc extracts the endpoint's resource identifier (roomID, targetUserID, or "").
type resourceIDFunc func(c *gin.Context) string

// botIdempotency is a Valkey-backed sentinel: SET NX per opID, then Del on non-5xx.
// 5xx keeps the sentinel so a retry cannot race the still-running original (TTL absorbs).
// Response body is NOT cached; downstream dedup keeps terminal state consistent.
func botIdempotency(
	client sentinelClient,
	siteID, endpoint string,
	sentinelTTL time.Duration,
	resourceIDFrom resourceIDFunc,
	tp timeProvider,
) gin.HandlerFunc {
	if tp == nil {
		tp = realTime{}
	}
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		pr := botPrincipalFrom(c)
		if pr == nil {
			errhttp.Write(ctx, c, errcode.Internal("bot idempotency: missing principal"))
			c.Abort()
			return
		}

		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			errhttp.Write(ctx, c, errcode.Internal("bot idempotency: read body", errcode.WithCause(err)))
			c.Abort()
			return
		}
		// Restore the body since Gin consumes Request.Body during ShouldBindJSON.
		c.Request.Body = io.NopCloser(bytes.NewReader(body))

		opID := computeOpID(siteID, endpoint, resourceIDFrom(c), pr.UserID, body, tp.Now())
		key := "idem:" + opID

		acquired, err := client.SetNX(ctx, key, "processing", sentinelTTL)
		if err != nil {
			errhttp.Write(ctx, c, errcode.Internal("bot idempotency: acquire", errcode.WithCause(err)))
			c.Abort()
			return
		}
		if !acquired {
			c.Header("Retry-After", "1")
			errhttp.Write(ctx, c, errBotInFlight)
			c.Abort()
			return
		}

		c.Next()

		// Release only on non-5xx; 5xx keeps the sentinel until TTL.
		if c.Writer.Status() < 500 {
			if err := client.Del(ctx, key); err != nil {
				// Best-effort; key still expires at sentinelTTL.
				slog.WarnContext(ctx, "bot idempotency sentinel delete failed",
					"key", key, "status", c.Writer.Status(), "error", err)
			}
		}
	}
}

// computeOpID = sha256(siteID:endpoint:resourceID:callerID:bodyHash:bucket).
// 60s bucket gives retry stability within a minute; older retries are fresh operations.
func computeOpID(siteID, endpoint, resourceID, callerID string, body []byte, now time.Time) string {
	bodySum := sha256.Sum256(body)
	bucket := now.Unix() / 60
	composite := fmt.Sprintf("%s:%s:%s:%s:%s:%s",
		siteID,
		endpoint,
		resourceID,
		callerID,
		hex.EncodeToString(bodySum[:]),
		strconv.FormatInt(bucket, 10),
	)
	sum := sha256.Sum256([]byte(composite))
	return hex.EncodeToString(sum[:])
}
