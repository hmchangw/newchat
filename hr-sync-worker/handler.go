package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
)

// Handler routes one HR-feed message by subject suffix. Malformed payloads
// are permanent (Ack-poison); store failures are transient (Nak-retry).
type Handler struct {
	store Store
}

func NewHandler(store Store) *Handler { return &Handler{store: store} }

func (h *Handler) HandleMessage(ctx context.Context, subj string, data []byte) error {
	switch {
	case strings.HasSuffix(subj, ".employees.upsert"):
		var employees []model.EmployeeWithChange
		if err := json.Unmarshal(data, &employees); err != nil {
			return errcode.Permanent(errcode.BadRequest(fmt.Sprintf("malformed employees.upsert payload: %s", err.Error())))
		}
		if len(employees) == 0 {
			return nil
		}
		if err := h.store.UpsertEmployees(ctx, employees); err != nil {
			return fmt.Errorf("upsert employees: %w", err)
		}
	case strings.HasSuffix(subj, ".users.upsert"):
		var users []model.UserWithChange
		if err := json.Unmarshal(data, &users); err != nil {
			return errcode.Permanent(errcode.BadRequest(fmt.Sprintf("malformed users.upsert payload: %s", err.Error())))
		}
		if len(users) == 0 {
			return nil
		}
		if err := h.store.UpsertUserIdentities(ctx, users); err != nil {
			return fmt.Errorf("upsert user identities: %w", err)
		}
	case strings.HasSuffix(subj, ".employees.quit"):
		var batch model.HRSyncEmployeeQuitBatch
		if err := json.Unmarshal(data, &batch); err != nil {
			return errcode.Permanent(errcode.BadRequest(fmt.Sprintf("malformed employees.quit payload: %s", err.Error())))
		}
		if len(batch.Accounts) == 0 {
			return nil
		}
		if err := h.store.QuitTeamsEmployees(ctx, batch.Accounts); err != nil {
			return fmt.Errorf("quit employees: %w", err)
		}
	default:
		// unknown subject under chat.hr.> — redelivery can't fix it
		return errcode.Permanent(errcode.BadRequest("unhandled hr subject " + subj))
	}
	return nil
}
