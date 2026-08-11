package service

import (
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/user-service/models"
)

// GetPermission reports whether the caller currently holds a whitelist
// permission. Identity comes only from {account} — never the body (design §7).
func (s *UserService) GetPermission(c *natsrouter.Context, req models.PermissionGetRequest) (*models.PermissionGetResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	key := model.PermissionKey(req.Permission)
	if !model.KnownPermission(key) {
		return nil, errcode.BadRequest("unknown permission", errcode.WithReason(errcode.PermissionUnknownKey))
	}
	latest, err := s.permissions.GetLatestGrant(c, s.siteID, key, account)
	if err != nil {
		return nil, fmt.Errorf("get latest permission grant: %w", err)
	}
	granted := model.EvaluateGrant(latest, time.Now().UTC())
	return &models.PermissionGetResponse{Permission: req.Permission, Granted: granted}, nil
}
