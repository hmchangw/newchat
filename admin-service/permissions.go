package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
)

// tzTaipei is the fixed date-interpretation rule (spec §8.1). Never time.Local.
var tzTaipei = time.FixedZone("UTC+8", 8*60*60)

const (
	auditActionPermissionGrant  = "permission.grant"
	auditActionPermissionRevoke = "permission.revoke"
)

// permissionRequest is the request body for POST /v1/admin/permissions.
// EffectiveFrom/ExpiresAt are pointers so the handler can distinguish
// "omitted" (nil) from "empty string" — required for the revoke-must-omit
// rule (spec §4.4 step 9). Granted is a pointer for the same reason: an
// omitted "granted" must be rejected, not silently default to false/revoke —
// see the missing-fields check below (no binding tag, to keep the reason
// grouped with applicantAccount/approverAccount as missing_permission_fields
// rather than the generic JSON-bind bad_request).
type permissionRequest struct {
	Permission       string   `json:"permission"`
	SubjectAccounts  []string `json:"subjectAccounts"`
	Granted          *bool    `json:"granted"`
	EffectiveFrom    *string  `json:"effectiveFrom"` // "2026-09-01"; nil on grant = now
	ExpiresAt        *string  `json:"expiresAt"`     // "2026-12-31"; required on grant
	ApplicantAccount string   `json:"applicantAccount"`
	ApproverAccount  string   `json:"approverAccount"`
	Reason           string   `json:"reason"`
}

type grantCreated struct {
	ID             string `json:"id"`
	SubjectAccount string `json:"subjectAccount"`
}

type createPermissionsResponse struct {
	Created           int            `json:"created"`
	DuplicatesIgnored []string       `json:"duplicatesIgnored"` // [] not null
	Grants            []grantCreated `json:"grants"`
}

// dedupPreserveOrder returns subjectAccounts with duplicates removed,
// keeping each account's first-occurrence position, plus the accounts that
// were dropped, in the order they were dropped. Never returns a nil
// duplicates slice — the response field must marshal as [], not null.
func dedupPreserveOrder(accounts []string) (deduped, duplicates []string) {
	seen := make(map[string]bool, len(accounts))
	deduped = make([]string, 0, len(accounts))
	duplicates = []string{}
	for _, a := range accounts {
		if seen[a] {
			duplicates = append(duplicates, a)
			continue
		}
		seen[a] = true
		deduped = append(deduped, a)
	}
	return deduped, duplicates
}

// parseWindow validates and converts the request window. Call ONLY when
// granted=true. fromStr nil means effective immediately (now). until is the
// day-after-untilStr at midnight tzTaipei — the half-open interval's
// exclusive end (spec §3.4).
func parseWindow(fromStr, untilStr *string, now time.Time) (from, until time.Time, err error) {
	if untilStr == nil {
		return time.Time{}, time.Time{}, errors.New("expiresAt is required for a grant")
	}

	untilDay, err := time.ParseInLocation("2006-01-02", *untilStr, tzTaipei)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("expiresAt must be YYYY-MM-DD: %w", err)
	}
	until = time.Date(untilDay.Year(), untilDay.Month(), untilDay.Day()+1, 0, 0, 0, 0, tzTaipei)

	if fromStr == nil {
		from = now
	} else {
		from, err = time.ParseInLocation("2006-01-02", *fromStr, tzTaipei)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("effectiveFrom must be YYYY-MM-DD: %w", err)
		}
	}

	// until-not-in-the-past is checked before from-vs-until: fromStr==nil makes
	// from default to now, which is always > a past until. Checking from-vs-until
	// first would then report a misleading "effectiveFrom must not be after
	// expiresAt" about a field the caller never sent.
	if !until.After(now) {
		return time.Time{}, time.Time{}, errors.New("expiresAt must not be in the past")
	}
	// until is already shifted +1 day from the caller's civil expiresAt, so
	// from==until (from is one full day past expiresAt) must be rejected too —
	// from.Before(until), not the weaker from.After(until)==false.
	if !from.Before(until) {
		return time.Time{}, time.Time{}, errors.New("effectiveFrom must not be after expiresAt")
	}

	return from, until, nil
}

// displayDate renders the civil date of t in tzTaipei ("2006-01-02") — the
// admin GET's rendering of EffectiveFrom.
func displayDate(t time.Time) string {
	return t.In(tzTaipei).Format("2006-01-02")
}

// displayUntilDate renders the inclusive end date: the civil date of t in
// tzTaipei minus one day. t is the stored half-open ExpiresAt instant
// (already +1 day from what the admin typed); this reverses that shift so
// the admin GET never shows the exclusive boundary (spec §3.4).
func displayUntilDate(t time.Time) string {
	return t.In(tzTaipei).AddDate(0, 0, -1).Format("2006-01-02")
}

// createPermissions handles POST /v1/admin/permissions — grants or revokes
// a permission for one or more subject accounts, all-or-nothing, with one
// audit entry per created row. Validation order matches spec §4.4 exactly.
func (h *Handler) createPermissions(c *gin.Context) {
	ctx := c.Request.Context()

	var req permissionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("invalid request body",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}

	if !model.KnownPermission(model.PermissionKey(req.Permission)) {
		errhttp.Write(ctx, c, errcode.BadRequest("unknown permission",
			errcode.WithReason(errcode.PermissionUnknownKey)))
		return
	}

	if len(req.SubjectAccounts) == 0 || len(req.SubjectAccounts) > model.MaxSubjects {
		errhttp.Write(ctx, c, errcode.BadRequest(fmt.Sprintf("subjectAccounts must contain 1 to %d accounts", model.MaxSubjects),
			errcode.WithReason(errcode.PermissionInvalidSubjects)))
		return
	}

	subjects, duplicatesIgnored := dedupPreserveOrder(req.SubjectAccounts)

	if utf8.RuneCountInString(req.Reason) > model.MaxReasonRunes {
		errhttp.Write(ctx, c, errcode.BadRequest(fmt.Sprintf("reason must be at most %d runes", model.MaxReasonRunes),
			errcode.WithReason(errcode.PermissionInvalidReason)))
		return
	}

	if req.Granted == nil || req.ApplicantAccount == "" || req.ApproverAccount == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("granted, applicantAccount, and approverAccount are required",
			errcode.WithReason(errcode.PermissionMissingFields)))
		return
	}
	granted := *req.Granted

	now := time.Now().UTC()

	var from, until time.Time
	if granted {
		var err error
		from, until, err = parseWindow(req.EffectiveFrom, req.ExpiresAt, now)
		if err != nil {
			errhttp.Write(ctx, c, errcode.BadRequest(err.Error(),
				errcode.WithReason(errcode.PermissionInvalidWindow)))
			return
		}
	} else if req.EffectiveFrom != nil || req.ExpiresAt != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("effectiveFrom and expiresAt must be absent on a revoke",
			errcode.WithReason(errcode.PermissionUnexpectedWindow)))
		return
	}

	// FindAccountStates must see every account this request references — subjects
	// plus applicant/approver, deduped into one batch (spec §4.4 step 10: "all
	// accounts exist at this site; subjects additionally pass IsActive()"). subjects
	// is already deduped above, so only applicant/approver can introduce repeats.
	checkAccounts, _ := dedupPreserveOrder(append(append(make([]string, 0, len(subjects)+2), subjects...), req.ApplicantAccount, req.ApproverAccount))

	states, err := h.store.FindAccountStates(ctx, h.cfg.SiteID, checkAccounts)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("find account states: %w", err))
		return
	}

	// Existence applies to subjects AND applicant/approver; IsActive() applies to
	// subjects only — an inactive applicant/approver is explicitly allowed (spec
	// §4.4 step 10; docs/client-api.md §9.13).
	subjectSet := make(map[string]bool, len(subjects))
	for _, acct := range subjects {
		subjectSet[acct] = true
	}
	var unknown, inactive []string
	for _, acct := range checkAccounts {
		active, exists := states[acct]
		if !exists {
			unknown = append(unknown, acct)
		} else if subjectSet[acct] && !active {
			inactive = append(inactive, acct)
		}
	}
	// Metadata "accounts" is CLIENT-VISIBLE by design (errcode doc.go) — the console
	// renders it so the admin sees exactly which accounts were rejected; the message
	// carries the same list for curl callers.
	if len(unknown) > 0 {
		errhttp.Write(ctx, c, errcode.NotFound("unknown accounts: "+strings.Join(unknown, ", "),
			errcode.WithReason(errcode.PermissionUnknownAccounts),
			errcode.WithMetadata("accounts", strings.Join(unknown, ", "))))
		return
	}
	if len(inactive) > 0 {
		errhttp.Write(ctx, c, errcode.BadRequest("inactive accounts cannot be granted a permission: "+strings.Join(inactive, ", "),
			errcode.WithReason(errcode.PermissionInactiveSubject),
			errcode.WithMetadata("accounts", strings.Join(inactive, ", "))))
		return
	}

	principal := principalFrom(c)
	grants := make([]*model.PermissionGrant, len(subjects))
	for i, acct := range subjects {
		g := &model.PermissionGrant{
			ID:               idgen.GenerateUUIDv7(),
			SiteID:           h.cfg.SiteID,
			Permission:       model.PermissionKey(req.Permission),
			SubjectAccount:   acct,
			Granted:          granted,
			ApplicantAccount: req.ApplicantAccount,
			ApproverAccount:  req.ApproverAccount,
			Reason:           req.Reason,
			RecordedBy:       principal.Account,
			RecordedAt:       now,
		}
		if granted {
			g.EffectiveFrom = &from
			g.ExpiresAt = &until
		}
		grants[i] = g
	}

	if err := h.store.InsertPermissionGrants(ctx, grants); err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("insert permission grants: %w", err))
		return
	}

	action := auditActionPermissionRevoke
	if granted {
		action = auditActionPermissionGrant
	}
	entries := make([]*AuditEntry, len(grants))
	for i, g := range grants {
		entries[i] = &AuditEntry{
			ID:            idgen.GenerateUUIDv7(),
			ActorUserID:   principal.UserID,
			ActorAccount:  principal.Account,
			Action:        action,
			TargetAccount: g.SubjectAccount,
			Details:       map[string]string{"permission": string(g.Permission)},
			SiteID:        h.cfg.SiteID,
			Timestamp:     now.UnixMilli(),
		}
	}
	if err := h.store.AppendAuditMany(ctx, entries); err != nil {
		slog.ErrorContext(ctx, "append permission audit entries failed", "action", action, "error", err)
	}

	resp := createPermissionsResponse{
		Created:           len(grants),
		DuplicatesIgnored: duplicatesIgnored,
		Grants:            make([]grantCreated, len(grants)),
	}
	for i, g := range grants {
		resp.Grants[i] = grantCreated{ID: g.ID, SubjectAccount: g.SubjectAccount}
	}
	c.JSON(http.StatusCreated, resp)
}

// permissionGrantView renders one ledger row for the admin GET: display
// dates (civil, tzTaipei), not raw instants — spec §3.4. Revoke rows have no
// window, so EffectiveFrom/ExpiresAt/ExpiresAtUTC stay "" and are omitted.
type permissionGrantView struct {
	ID               string    `json:"id"`
	Permission       string    `json:"permission"`
	SubjectAccount   string    `json:"subjectAccount"`
	Granted          bool      `json:"granted"`
	EffectiveFrom    string    `json:"effectiveFrom,omitempty"` // "2026-09-01"; empty on revoke rows
	ExpiresAt        string    `json:"expiresAt,omitempty"`     // "2026-12-31"; empty on revoke rows
	ExpiresAtUTC     string    `json:"expiresAtUTC,omitempty"`  // RFC3339 exact stored instant
	ApplicantAccount string    `json:"applicantAccount"`
	ApproverAccount  string    `json:"approverAccount"`
	Reason           string    `json:"reason"`
	RecordedBy       string    `json:"recordedBy"`
	RecordedAt       time.Time `json:"recordedAt"`
}

type listPermissionsResponse struct {
	// CurrentlyGranted is present ONLY when BOTH subjectAccount and permission
	// query params were supplied — a latest-wins decision is meaningless
	// without both (there is no single decision across multiple subjects or
	// across multiple permissions).
	CurrentlyGranted *bool                 `json:"currentlyGranted,omitempty"`
	Entries          []permissionGrantView `json:"entries"`
	Total            int64                 `json:"total"`
}

// toPermissionGrantView converts a stored grant row to its display form.
func toPermissionGrantView(g *model.PermissionGrant) permissionGrantView {
	v := permissionGrantView{
		ID:               g.ID,
		Permission:       string(g.Permission),
		SubjectAccount:   g.SubjectAccount,
		Granted:          g.Granted,
		ApplicantAccount: g.ApplicantAccount,
		ApproverAccount:  g.ApproverAccount,
		Reason:           g.Reason,
		RecordedBy:       g.RecordedBy,
		RecordedAt:       g.RecordedAt,
	}
	if g.EffectiveFrom != nil {
		v.EffectiveFrom = displayDate(*g.EffectiveFrom)
	}
	if g.ExpiresAt != nil {
		v.ExpiresAt = displayUntilDate(*g.ExpiresAt)
		v.ExpiresAtUTC = g.ExpiresAt.UTC().Format(time.RFC3339)
	}
	return v
}

// listPermissions handles GET /v1/admin/permissions — returns the ledger for
// the site, newest-first, optionally filtered by subjectAccount and/or
// permission (both independently optional), plus the current computed
// decision when both filters narrow to a single subject+permission (spec
// §4.6).
func (h *Handler) listPermissions(c *gin.Context) {
	ctx := c.Request.Context()

	subjectAccount := c.Query("subjectAccount")

	permission := model.PermissionKey(c.Query("permission"))
	if permission != "" && !model.KnownPermission(permission) {
		errhttp.Write(ctx, c, errcode.BadRequest("unknown permission",
			errcode.WithReason(errcode.PermissionUnknownKey)))
		return
	}

	page, limit := parsePaging(c, 1, 20)

	grants, total, err := h.store.ListPermissionGrants(ctx, h.cfg.SiteID, subjectAccount, permission, page, limit)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("list permission grants: %w", err))
		return
	}

	entries := make([]permissionGrantView, len(grants))
	for i := range grants {
		entries[i] = toPermissionGrantView(&grants[i])
	}

	resp := listPermissionsResponse{
		Entries: entries,
		Total:   total,
	}

	// currentlyGranted is a latest-wins decision over (permission, subjectAccount) —
	// meaningless without both, so it's computed and included only when the
	// caller supplied both filters (spec §4.6).
	if permission != "" && subjectAccount != "" {
		latest, err := h.store.GetLatestPermissionGrant(ctx, h.cfg.SiteID, permission, subjectAccount)
		if err != nil {
			errhttp.Write(ctx, c, fmt.Errorf("get latest permission grant: %w", err))
			return
		}
		decision := model.EvaluateGrant(latest, time.Now().UTC())
		resp.CurrentlyGranted = &decision
	}

	c.JSON(http.StatusOK, resp)
}
