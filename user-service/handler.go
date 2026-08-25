package main

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/user-service/models"
)

type handler struct {
	subs         subscriptionLister
	defaultLimit int
	maxLimit     int
}

// newHandler injects page bounds: HTTP and NATS carry different ceilings.
func newHandler(subs subscriptionLister, defaultLimit, maxLimit int) *handler {
	return &handler{subs: subs, defaultLimit: defaultLimit, maxLimit: maxLimit}
}

// listQuery mirrors models.SubscriptionListRequest. The pointer fields keep
// "omitted" distinct from the zero value: an omitted includeLastMessage means
// include, so binding it as a plain bool would invert the default.
type listQuery struct {
	Type               string `form:"type"`
	Favorite           *bool  `form:"favorite"`
	UpdatedWithinDays  *int   `form:"updatedWithinDays"`
	IncludeLastMessage *bool  `form:"includeLastMessage"`
	Offset             int    `form:"offset"`
	Limit              int    `form:"limit"`
}

// ListSubscriptions serves GET /api/v1/subscriptions for the authenticated
// caller. Validation and page clamping belong to the service, which both
// transports share.
func (h *handler) ListSubscriptions(c *gin.Context) {
	ctx := c.Request.Context()

	var q listQuery
	if err := c.ShouldBindQuery(&q); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("invalid query parameters", errcode.WithCause(err)))
		return
	}

	resp, err := h.subs.ListSubscriptionsFor(ctx, accountFromContext(c), models.SubscriptionListRequest{
		Type:               q.Type,
		Favorite:           q.Favorite,
		UpdatedWithinDays:  q.UpdatedWithinDays,
		IncludeLastMessage: q.IncludeLastMessage,
		Offset:             q.Offset,
		Limit:              q.Limit,
	}, h.defaultLimit, h.maxLimit)
	if err != nil {
		errhttp.Write(ctx, c, err)
		return
	}
	// The credential is a non-standard header, so Vary cannot protect this: a cache
	// keyed on method+URL would serve one account's sidebar to another.
	c.Header("Cache-Control", "private, no-store")
	c.JSON(http.StatusOK, resp)
}
