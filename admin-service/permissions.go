package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"golang.org/x/sync/errgroup"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
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

type createPermissionsResponse struct {
	Created           int      `json:"created"`
	DuplicatesIgnored []string `json:"duplicatesIgnored"` // [] not null
	// SyncFailures lists remote sites whose cross-site fanout publish did not land —
	// omitted (not []) when every destination succeeded. The ledger write already
	// committed regardless; this only flags which peers may be out of sync.
	SyncFailures []string `json:"syncFailures,omitempty"`
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
	return displayDate(t.AddDate(0, 0, -1))
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

	if len(req.SubjectAccounts) == 0 {
		errhttp.Write(ctx, c, errcode.BadRequest("subjectAccounts must not be empty",
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
	// plus applicant/approver (spec §4.4 step 10: "all accounts exist at this site;
	// subjects additionally pass IsActive()"). subjects is already deduped above, so
	// only applicant/approver can repeat an entry; $in and the returned map tolerate it.
	checkAccounts := append(slices.Clone(subjects), req.ApplicantAccount, req.ApproverAccount)

	states, err := h.store.FindAccountStates(ctx, checkAccounts)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("find account states: %w", err))
		return
	}

	// Existence applies to subjects AND applicant/approver; IsActive() applies to
	// subjects only — an inactive applicant/approver is explicitly allowed (spec
	// §4.4 step 10; docs/client-api.md §9.13). The subjects lead checkAccounts, so
	// i < len(subjects) is the is-a-subject test, and seen keeps an applicant or
	// approver that repeats an earlier entry from being reported twice.
	seen := make(map[string]bool, len(checkAccounts))
	var unknown, inactive []string
	for i, acct := range checkAccounts {
		if seen[acct] {
			continue
		}
		seen[acct] = true
		switch active, exists := states[acct]; {
		case !exists:
			unknown = append(unknown, acct)
		case i < len(subjects) && !active:
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

	key := model.PermissionKey(req.Permission)
	principal := principalFrom(c)
	grants := make([]*model.PermissionGrant, len(subjects))
	for i, acct := range subjects {
		g := &model.PermissionGrant{
			ID:               idgen.GenerateUUIDv7(),
			Permission:       key,
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

	state := model.PermissionState{Granted: *req.Granted, UpdatedAt: now}
	if *req.Granted {
		state.EffectiveFrom = &from
		state.ExpiresAt = &until
	}
	if err := h.store.RecordPermissionChange(ctx, grants, state); err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("record permission change: %w", err))
		return
	}

	// Audit BEFORE the fanout, not after: the fanout can burn its full budget on an
	// unreachable peer, and this runs on the request ctx — a client disconnect during
	// that window would cancel the audit write and lose every entry for a ledger write
	// that already committed. Best-effort either way: a failure is logged, never fatal.
	action := auditActionPermissionRevoke
	if granted {
		action = auditActionPermissionGrant
	}
	entries := make([]*AuditEntry, len(grants))
	for i, g := range grants {
		entries[i] = h.auditEntry(c, action, "", g.SubjectAccount,
			map[string]string{"permission": string(g.Permission)}, now)
	}
	if err := h.store.AppendAuditMany(ctx, entries); err != nil {
		slog.ErrorContext(ctx, "append permission audit entries failed", "action", action, "error", err)
	}

	fanoutCtx, cancelFanout := fanoutContext(ctx)
	defer cancelFanout()
	syncFailures := h.publishPermissionFanout(fanoutCtx, key, subjects, state)

	c.JSON(http.StatusCreated, createPermissionsResponse{
		Created:           len(grants),
		DuplicatesIgnored: duplicatesIgnored,
		SyncFailures:      syncFailures,
	})
}

// fanoutChunkSize bounds one UserPermissionsUpdated event's Accounts so a worst-case
// event stays ~320KB — 3× margin under NATS's default 1MB max_payload.
const fanoutChunkSize = 5000

// fanoutTimeout is the server-side budget for ONE request's whole cross-site fanout,
// however many publishPermissionFanout calls that takes — resync makes one per state
// group, and a per-call budget would multiply wall time by the group count until it
// exceeded httpWriteTimeout and the admin lost the syncFailures reply.
const fanoutTimeout = 5 * time.Second

// fanoutContext derives the fanout budget from a request ctx. The request ctx has no
// deadline and dies if the client disconnects — neither fits: the ledger write already
// committed, so fanout runs to completion on its own budget. A destination that doesn't
// land within it is reported in syncFailures like any other unacknowledged publish.
func fanoutContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), fanoutTimeout)
}

// publishPermissionFanout publishes the batch to every remote site's INBOX, one event
// per site per chunk of accounts. Failures are logged and aggregated per destination and
// never abort the fanout — one dead peer must not block the others. Returns the
// destinations whose publish was not acknowledged (nil when all landed). ctx must come
// from fanoutContext — the caller owns the budget so repeated calls share one.
func (h *Handler) publishPermissionFanout(ctx context.Context, permission model.PermissionKey, accounts []string, state model.PermissionState) []string {
	dests := remoteSites(h.cfg.AllSiteIDs, h.cfg.SiteID)
	if h.publishInbox == nil || len(accounts) == 0 || len(dests) == 0 {
		return nil
	}

	now := time.Now().UTC().UnixMilli()
	// Marshal each chunk's payload once; only the envelope differs per destination.
	var chunks [][]byte
	for start := 0; start < len(accounts); start += fanoutChunkSize {
		end := min(start+fanoutChunkSize, len(accounts))
		payload, err := json.Marshal(model.UserPermissionsUpdated{
			Permission: permission,
			Accounts:   accounts[start:end],
			State:      state,
			Timestamp:  now,
		})
		if err != nil {
			// Cannot serialize the event at all: every destination is out of sync.
			slog.ErrorContext(ctx, "marshal user permissions event", "error", err)
			return dests
		}
		chunks = append(chunks, payload)
	}

	// One goroutine per destination. Destinations are independent and share ONE budget
	// (fanoutContext), so publishing them serially lets a single unreachable peer burn
	// the whole budget and hand every peer behind it an already-expired ctx — reporting
	// healthy sites as sync failures. Results land in a position-indexed slice, so no
	// lock is needed and the reported order stays the configured destination order.
	failed := make([]bool, len(dests))
	var g errgroup.Group
	for i, dest := range dests {
		g.Go(func() error {
			for _, payload := range chunks {
				data, err := json.Marshal(model.InboxEvent{
					Type:       model.InboxUserPermissionsUpdated,
					SiteID:     h.cfg.SiteID,
					DestSiteID: dest,
					Payload:    payload,
					Timestamp:  now,
				})
				if err != nil {
					slog.ErrorContext(ctx, "marshal permissions inbox envelope", "dest", dest, "error", err)
					failed[i] = true
					continue
				}
				if err := h.publishInbox(ctx, subject.InboxExternal(dest, model.InboxUserPermissionsUpdated), data); err != nil {
					// Not self-healing like status/settings: surface it, don't swallow it.
					slog.ErrorContext(ctx, "publish permissions inbox event", "dest", dest, "error", err)
					failed[i] = true
				}
			}
			return nil
		})
	}
	_ = g.Wait() // every goroutine returns nil; per-destination outcomes are in failed

	var failures []string
	for i, dest := range dests {
		if failed[i] {
			failures = append(failures, dest)
		}
	}
	return failures
}

// remoteSites filters allSiteIDs down to real remote destinations.
func remoteSites(allSiteIDs []string, self string) []string {
	var out []string
	for _, dest := range allSiteIDs {
		if dest != "" && dest != self {
			out = append(out, dest)
		}
	}
	return out
}

type resyncPermissionsRequest struct {
	Permission string   `json:"permission"`
	Accounts   []string `json:"accounts"`
}

type resyncPermissionsResponse struct {
	SyncFailures []string `json:"syncFailures,omitempty"`
}

// resyncPermissions re-delivers the current materialized state for the given accounts —
// re-delivery, not re-recording: it writes nothing (no ledger, no audit, no user docs)
// and is idempotent (delivered states keep their stored watermarks, so remote guards
// no-op anything already applied).
func (h *Handler) resyncPermissions(c *gin.Context) {
	ctx := c.Request.Context()
	var req resyncPermissionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("invalid request body", errcode.WithReason(errcode.AuthMissingFields)))
		return
	}
	key := model.PermissionKey(req.Permission)
	if !model.KnownPermission(key) {
		errhttp.Write(ctx, c, errcode.BadRequest("unknown permission", errcode.WithReason(errcode.PermissionUnknownKey)))
		return
	}
	accounts, _ := dedupPreserveOrder(req.Accounts)
	if len(accounts) == 0 {
		errhttp.Write(ctx, c, errcode.BadRequest("accounts must be non-empty", errcode.WithReason(errcode.PermissionInvalidSubjects)))
		return
	}
	perms, err := h.store.GetUserPermissionsForAccounts(ctx, accounts)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("load user permissions: %w", err))
		return
	}
	// Group by the state's canonical JSON: PermissionState holds pointers, so it can't
	// be a map key itself. Field order is fixed by the struct and a nil bound (omitted)
	// serializes distinctly from any set bound, so distinct states never collide; the
	// worst case for a future field is one extra — and idempotent — fanout group.
	groups := make(map[string][]string)
	states := make(map[string]model.PermissionState)
	for _, account := range accounts {
		st := perms[account].State(key)
		if st == nil {
			continue // no doc or nothing recorded — nothing to sync
		}
		// Error discarded: PermissionState is bools, times, and pointers to them.
		k, _ := json.Marshal(st)
		groups[string(k)] = append(groups[string(k)], account)
		states[string(k)] = *st
	}
	// ONE budget for the whole resync, not one per group: groups are keyed on
	// UpdatedAt, so accounts granted across N admin submits yield N groups, and a
	// per-group budget would stretch an unreachable peer's wall time to N × the
	// budget — past httpWriteTimeout, which drops the connection before the admin
	// can be told which peers to retry.
	fanoutCtx, cancel := fanoutContext(ctx)
	defer cancel()
	seen := make(map[string]struct{})
	var failures []string
	for k, groupAccounts := range groups {
		for _, dest := range h.publishPermissionFanout(fanoutCtx, key, groupAccounts, states[k]) {
			if _, dup := seen[dest]; !dup {
				seen[dest] = struct{}{}
				failures = append(failures, dest)
			}
		}
	}
	c.JSON(http.StatusOK, resyncPermissionsResponse{SyncFailures: failures})
}

// permissionGrantView renders one ledger row for the admin GET: display
// dates (civil, tzTaipei), not raw instants — spec §3.4. Revoke rows have no
// window, so EffectiveFrom/ExpiresAt stay "" and are omitted.
type permissionGrantView struct {
	ID               string    `json:"id"`
	Permission       string    `json:"permission"`
	SubjectAccount   string    `json:"subjectAccount"`
	Granted          bool      `json:"granted"`
	EffectiveFrom    string    `json:"effectiveFrom,omitempty"` // "2026-09-01"; empty on revoke rows
	ExpiresAt        string    `json:"expiresAt,omitempty"`     // "2026-12-31"; empty on revoke rows
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
	}
	return v
}

// listPermissions handles GET /v1/admin/permissions — returns the ledger
// company-wide, newest-first, optionally filtered by subjectAccount and/or
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

	grants, total, err := h.store.ListPermissionGrants(ctx, subjectAccount, permission, page, limit)
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

	// currentlyGranted reads the materialized per-account permission snapshot —
	// meaningless without both filters (there's no single decision across
	// multiple subjects or multiple permissions), so it's computed and included
	// only when the caller supplied both (spec §4.6).
	if permission != "" && subjectAccount != "" {
		perms, err := h.store.GetUserPermissions(ctx, subjectAccount)
		if err != nil {
			errhttp.Write(ctx, c, fmt.Errorf("get user permissions: %w", err))
			return
		}
		granted := perms.Evaluated(time.Now().UTC())[permission]
		resp.CurrentlyGranted = &granted
	}

	c.JSON(http.StatusOK, resp)
}
