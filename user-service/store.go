package main

import (
	"context"

	"github.com/hmchangw/chat/user-service/models"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// subscriptionLister is the slice of the service layer the HTTP handler needs.
// Declared in the consumer so the handler can be tested without the service.
type subscriptionLister interface {
	ListSubscriptionsFor(ctx context.Context, account string, req models.SubscriptionListRequest, defaultLimit, maxLimit int) (*models.PagedSubscriptionListResponse, error)
}
