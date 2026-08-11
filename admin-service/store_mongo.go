package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/session"
)

type storeMongo struct {
	users      *mongo.Collection
	adminAudit *mongo.Collection
	permGrants *mongo.Collection
}

func newStoreMongo(db *mongo.Database) *storeMongo {
	return &storeMongo{
		users:      db.Collection("users"),
		adminAudit: db.Collection("admin_audit"),
		permGrants: db.Collection("permission_grants"),
	}
}

// EnsureIndexes creates required indexes idempotently.
func (s *storeMongo) EnsureIndexes(ctx context.Context) error {
	// Matches botplatform's auto-named "account_1" unique index for idempotency on the shared collection.
	_, err := s.users.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "account", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("create users account index: %w", err)
	}

	_, err = s.adminAudit.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "siteId", Value: 1}, {Key: "timestamp", Value: -1}},
	})
	if err != nil {
		return fmt.Errorf("create admin_audit siteId_timestamp index: %w", err)
	}

	// Backs the ListAudit `targetAccount` filter (audit entries are keyed by
	// account, not internal user ID).
	_, err = s.adminAudit.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "siteId", Value: 1}, {Key: "targetAccount", Value: 1}, {Key: "timestamp", Value: -1}},
	})
	if err != nil {
		return fmt.Errorf("create admin_audit siteId_targetAccount_timestamp index: %w", err)
	}

	// Backs ListPermissionGrants/GetLatestPermissionGrant: equality prefix
	// (siteId, permission, subjectAccount) + sort suffix (recordedAt, _id) so
	// newest-first ordering comes free from the index (spec §3.6).
	_, err = s.permGrants.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "siteId", Value: 1},
			{Key: "permission", Value: 1},
			{Key: "subjectAccount", Value: 1},
			{Key: "recordedAt", Value: -1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		return fmt.Errorf("create permission_grants siteId_permission_subjectAccount_recordedAt_id index: %w", err)
	}

	// Backs the audit/BI browse (no subjectAccount equality, so index 1 above doesn't apply).
	_, err = s.permGrants.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "siteId", Value: 1}, {Key: "recordedAt", Value: -1}},
	})
	if err != nil {
		return fmt.Errorf("create permission_grants siteId_recordedAt index: %w", err)
	}

	return nil
}

// userProjection contains fields returned for user management operations.
// Services.password is intentionally excluded — credential material never leaves the store.
var userProjection = bson.M{
	"_id":                   1,
	"account":               1,
	"siteId":                1,
	"sectId":                1,
	"sectName":              1,
	"sectTCName":            1,
	"sectDescription":       1,
	"deptId":                1,
	"deptName":              1,
	"deptTCName":            1,
	"deptDescription":       1,
	"engName":               1,
	"chineseName":           1,
	"employeeId":            1,
	"statusIsShow":          1,
	"statusText":            1,
	"roles":                 1,
	"requirePasswordChange": 1,
	"active":                1,
}

func (s *storeMongo) SearchUsers(ctx context.Context, siteID, q string, page, limit int) ([]model.User, int64, error) {
	filter := bson.M{"siteId": siteID}
	if q != "" {
		// Escape so the query is matched as a literal substring, not a regex
		// pattern — prevents metacharacter injection and ReDoS-style DoS.
		escaped := regexp.QuoteMeta(q)
		filter["$or"] = bson.A{
			bson.M{"account": bson.M{"$regex": escaped, "$options": "i"}},
			bson.M{"engName": bson.M{"$regex": escaped, "$options": "i"}},
			bson.M{"chineseName": bson.M{"$regex": escaped, "$options": "i"}},
		}
	}

	skip := int64((page - 1) * limit)

	total, err := s.users.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, fmt.Errorf("count users: %w", err)
	}

	cur, err := s.users.Find(ctx, filter,
		options.Find().
			SetProjection(userProjection).
			SetSkip(skip).
			SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("find users: %w", err)
	}

	var users []model.User
	if err := cur.All(ctx, &users); err != nil {
		return nil, 0, fmt.Errorf("decode users: %w", err)
	}
	if users == nil {
		users = []model.User{}
	}
	return users, total, nil
}

func (s *storeMongo) GetUserByAccount(ctx context.Context, siteID, account string) (*model.User, error) {
	var u model.User
	err := s.users.FindOne(ctx, bson.M{"siteId": siteID, "account": account},
		options.FindOne().SetProjection(userProjection),
	).Decode(&u)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("get user by account: %w", err)
	}
	return &u, nil
}

// userAuthProjection contains fields returned for the login/change-password
// paths, including the bcrypt hash needed for pwhash.Verify. Never used by
// admin management endpoints — those use userProjection, which excludes it.
var userAuthProjection = bson.M{
	"_id":                   1,
	"account":               1,
	"siteId":                1,
	"roles":                 1,
	"requirePasswordChange": 1,
	"active":                1,
	"services":              1,
}

// GetUserForAuth loads a user with credential material for the login/change-password paths. Not exposed to admin management endpoints.
func (s *storeMongo) GetUserForAuth(ctx context.Context, siteID, account string) (*model.User, error) {
	var u model.User
	err := s.users.FindOne(ctx, bson.M{"siteId": siteID, "account": account},
		options.FindOne().SetProjection(userAuthProjection),
	).Decode(&u)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("get user for auth: %w", err)
	}
	return &u, nil
}

func (s *storeMongo) CreateUser(ctx context.Context, u *model.User) error {
	_, err := s.users.InsertOne(ctx, u)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return ErrAccountExists
		}
		return fmt.Errorf("insert user: %w", err)
	}
	return nil
}

func (s *storeMongo) UpdateUser(ctx context.Context, siteID, account string, fields UserUpdate) error {
	set := bson.M{}
	if fields.EngName != nil {
		set["engName"] = *fields.EngName
	}
	if fields.ChineseName != nil {
		set["chineseName"] = *fields.ChineseName
	}
	if fields.Roles != nil {
		set["roles"] = *fields.Roles
	}
	if fields.Active != nil {
		set["active"] = *fields.Active
	}
	if len(set) == 0 {
		return nil
	}

	filter := bson.M{"account": account, "siteId": siteID}

	// Deactivation no longer flows through UpdateUser — the handler routes
	// active=false to DeactivateAndRevoke instead so the user-flag flip
	// and session-purge run in one Mongo transaction. UpdateUser stays
	// non-transactional for the remaining patch fields (roles, names).
	result, err := s.users.UpdateOne(ctx, filter, bson.M{"$set": set})
	if err != nil {
		return fmt.Errorf("update user: %w", err)
	}
	if result.MatchedCount == 0 {
		return ErrUserNotFound
	}
	return nil
}

// withTransaction runs fn inside a Mongo multi-document transaction. Requires a
// replica-set deployment (production, and the RS container in integration tests).
// The driver retries fn on transient transaction errors.
func (s *storeMongo) withTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	sess, err := s.users.Database().Client().StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer sess.EndSession(ctx)
	_, err = sess.WithTransaction(ctx, func(ctx context.Context) (any, error) {
		return nil, fn(ctx)
	})
	return err
}

// UpdateUserPasswordAndRevoke atomically replaces the bcrypt hash +
// requirePasswordChange flag and deletes matching sessions for the account, so
// a leaked old credential cannot keep a session alive after the reset.
// exceptSessionID, when non-empty, is excluded from the revoke (self-service
// change-password keeps the caller logged in); empty revokes every session
// for the account (admin-forced password set). Requires a replica set.
func (s *storeMongo) UpdateUserPasswordAndRevoke(ctx context.Context, siteID, account, bcryptHash string, requireChange bool, exceptSessionID string) error {
	userFilter := bson.M{"account": account, "siteId": siteID}
	sessionFilter := bson.M{"siteId": siteID, "account": account}
	if exceptSessionID != "" {
		sessionFilter["_id"] = bson.M{"$ne": exceptSessionID}
	}

	return s.withTransaction(ctx, func(ctx context.Context) error {
		result, err := s.users.UpdateOne(ctx, userFilter,
			bson.M{"$set": bson.M{
				"services.password.bcrypt": bcryptHash,
				"requirePasswordChange":    requireChange,
			}},
		)
		if err != nil {
			return fmt.Errorf("update user password: %w", err)
		}
		if result.MatchedCount == 0 {
			return ErrUserNotFound
		}
		sessions := s.users.Database().Collection(session.Collection)
		if _, err := sessions.DeleteMany(ctx, sessionFilter); err != nil {
			return fmt.Errorf("revoke sessions: %w", err)
		}
		return nil
	})
}

// DeactivateAndRevoke atomically sets active=false on the user and
// deletes every session for the account, so a disabled account can't keep a
// live token. Requires a replica set.
func (s *storeMongo) DeactivateAndRevoke(ctx context.Context, siteID, account string) error {
	filter := bson.M{"account": account, "siteId": siteID}

	return s.withTransaction(ctx, func(ctx context.Context) error {
		result, err := s.users.UpdateOne(ctx, filter, bson.M{"$set": bson.M{"active": false}})
		if err != nil {
			return fmt.Errorf("deactivate user: %w", err)
		}
		if result.MatchedCount == 0 {
			return ErrUserNotFound
		}
		sessions := s.users.Database().Collection(session.Collection)
		if _, err := sessions.DeleteMany(ctx, filter); err != nil {
			return fmt.Errorf("revoke sessions: %w", err)
		}
		return nil
	})
}

// auditProjection returns all audit entry fields.
var auditProjection = bson.M{
	"_id":           1,
	"actorUserId":   1,
	"actorAccount":  1,
	"action":        1,
	"targetUserId":  1,
	"targetAccount": 1,
	"details":       1,
	"siteId":        1,
	"timestamp":     1,
}

func (s *storeMongo) AppendAudit(ctx context.Context, e *AuditEntry) error {
	_, err := s.adminAudit.InsertOne(ctx, e)
	if err != nil {
		return fmt.Errorf("insert audit entry: %w", err)
	}
	return nil
}

// ListAudit returns audit entries newest-first, scoped to siteID, with optional
// filters on targetUserId, actorAccount, and action.
func (s *storeMongo) ListAudit(ctx context.Context, siteID string, f AuditFilter, page, limit int) ([]AuditEntry, int64, error) {
	filter := bson.M{"siteId": siteID}
	if f.TargetAccount != "" {
		filter["targetAccount"] = f.TargetAccount
	}
	if f.Actor != "" {
		filter["actorAccount"] = f.Actor
	}
	if f.Action != "" {
		filter["action"] = f.Action
	}

	skip := int64((page - 1) * limit)

	total, err := s.adminAudit.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, fmt.Errorf("count audit entries: %w", err)
	}

	cur, err := s.adminAudit.Find(ctx, filter,
		options.Find().
			SetProjection(auditProjection).
			SetSort(bson.D{{Key: "timestamp", Value: -1}}).
			SetSkip(skip).
			SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("find audit entries: %w", err)
	}

	var entries []AuditEntry
	if err := cur.All(ctx, &entries); err != nil {
		return nil, 0, fmt.Errorf("decode audit entries: %w", err)
	}
	if entries == nil {
		entries = []AuditEntry{}
	}
	return entries, total, nil
}

// permissionGrantProjection returns every PermissionGrant field explicitly —
// house rule "always project precisely" (CLAUDE.md Coding Rules); the admin
// GET renders the full ledger row, so nothing is trimmed.
var permissionGrantProjection = bson.M{
	"_id":              1,
	"siteId":           1,
	"permission":       1,
	"subjectAccount":   1,
	"granted":          1,
	"effectiveFrom":    1,
	"expiresAt":        1,
	"applicantAccount": 1,
	"approverAccount":  1,
	"reason":           1,
	"recordedBy":       1,
	"recordedAt":       1,
}

// InsertPermissionGrants appends the batch atomically (withTransaction +
// InsertMany), so a resend after a partial failure cannot produce duplicate
// rows (spec §4.4 note on step 11).
func (s *storeMongo) InsertPermissionGrants(ctx context.Context, grants []*model.PermissionGrant) error {
	if len(grants) == 0 {
		return nil
	}
	docs := make([]any, len(grants))
	for i, g := range grants {
		docs[i] = g
	}
	return s.withTransaction(ctx, func(ctx context.Context) error {
		if _, err := s.permGrants.InsertMany(ctx, docs); err != nil {
			return fmt.Errorf("insert permission grants: %w", err)
		}
		return nil
	})
}

// ListPermissionGrants returns the ledger for the site newest-first
// (recordedAt desc, _id desc). subjectAccount == "" means all subjects;
// permission == "" means all permissions — the two filters combine
// independently (spec §4.6). Omitting either or both breaks the equality
// prefix on index 1, so this path falls back to index 2 (siteId, recordedAt)
// plus a residual filter, or — when both are omitted — index 2 alone with no
// residual filter; accepted per spec.
func (s *storeMongo) ListPermissionGrants(ctx context.Context, siteID, subjectAccount string, permission model.PermissionKey, page, limit int) ([]model.PermissionGrant, int64, error) {
	filter := bson.M{"siteId": siteID}
	if subjectAccount != "" {
		filter["subjectAccount"] = subjectAccount
	}
	if permission != "" {
		filter["permission"] = permission
	}

	skip := int64((page - 1) * limit)

	total, err := s.permGrants.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, fmt.Errorf("count permission grants: %w", err)
	}

	cur, err := s.permGrants.Find(ctx, filter,
		options.Find().
			SetProjection(permissionGrantProjection).
			SetSort(bson.D{{Key: "recordedAt", Value: -1}, {Key: "_id", Value: -1}}).
			SetSkip(skip).
			SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("find permission grants: %w", err)
	}

	var grants []model.PermissionGrant
	if err := cur.All(ctx, &grants); err != nil {
		return nil, 0, fmt.Errorf("decode permission grants: %w", err)
	}
	if grants == nil {
		grants = []model.PermissionGrant{}
	}
	return grants, total, nil
}

// GetLatestPermissionGrant returns the newest grant row for the triple, or
// (nil, nil) when none exists — latest-wins evaluation reads this row
// through model.EvaluateGrant (spec §3.5).
func (s *storeMongo) GetLatestPermissionGrant(ctx context.Context, siteID string, permission model.PermissionKey, subjectAccount string) (*model.PermissionGrant, error) {
	var g model.PermissionGrant
	err := s.permGrants.FindOne(ctx,
		bson.M{"siteId": siteID, "permission": permission, "subjectAccount": subjectAccount},
		options.FindOne().
			SetProjection(permissionGrantProjection).
			SetSort(bson.D{{Key: "recordedAt", Value: -1}, {Key: "_id", Value: -1}}),
	).Decode(&g)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get latest permission grant: %w", err)
	}
	return &g, nil
}

// accountStateProjection contains only the fields FindAccountStates needs to
// derive IsActive() — never the rest of the user document.
var accountStateProjection = bson.M{"account": 1, "active": 1}

// FindAccountStates returns account -> IsActive() for the accounts that
// exist at the site; accounts not present in the map do not exist. One
// query for the whole batch (spec §4.4 step 10), rather than N lookups.
func (s *storeMongo) FindAccountStates(ctx context.Context, siteID string, accounts []string) (map[string]bool, error) {
	cur, err := s.users.Find(ctx,
		bson.M{"siteId": siteID, "account": bson.M{"$in": accounts}},
		options.Find().SetProjection(accountStateProjection),
	)
	if err != nil {
		return nil, fmt.Errorf("find account states: %w", err)
	}

	var rows []model.User
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode account states: %w", err)
	}

	states := make(map[string]bool, len(rows))
	for i := range rows {
		states[rows[i].Account] = rows[i].IsActive()
	}
	return states, nil
}

// AppendAuditMany inserts all entries in one InsertMany — a 200-subject
// batch would otherwise cost 200 round trips through AppendAudit. Same
// best-effort contract: the caller logs a failure, never fails the request.
func (s *storeMongo) AppendAuditMany(ctx context.Context, entries []*AuditEntry) error {
	if len(entries) == 0 {
		return nil
	}
	docs := make([]any, len(entries))
	for i, e := range entries {
		docs[i] = e
	}
	if _, err := s.adminAudit.InsertMany(ctx, docs); err != nil {
		return fmt.Errorf("insert audit entries: %w", err)
	}
	return nil
}

func (s *storeMongo) Ping(ctx context.Context) error {
	return s.users.Database().Client().Ping(ctx, nil)
}
