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
		var batch model.EmployeesUpsertBatch
		if err := json.Unmarshal(data, &batch); err != nil {
			return errcode.Permanent(errcode.BadRequest("malformed employees.upsert payload"))
		}
		if len(batch.Employees) == 0 {
			return nil
		}
		if err := h.store.UpsertEmployees(ctx, batch.Employees); err != nil {
			return fmt.Errorf("upsert employees: %w", err)
		}
	case strings.HasSuffix(subj, ".users.upsert"):
		var batch model.UsersUpsertBatch
		if err := json.Unmarshal(data, &batch); err != nil {
			return errcode.Permanent(errcode.BadRequest("malformed users.upsert payload"))
		}
		if len(batch.Users) == 0 {
			return nil
		}
		if err := h.store.UpsertUserIdentities(ctx, batch.Users); err != nil {
			return fmt.Errorf("upsert user identities: %w", err)
		}
	case strings.HasSuffix(subj, ".employees.quit"):
		var batch model.HRSyncEmployeeQuitBatch
		if err := json.Unmarshal(data, &batch); err != nil {
			return errcode.Permanent(errcode.BadRequest("malformed employees.quit payload"))
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
