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
	"github.com/hmchangw/chat/pkg/mongoutil"
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
	// users.account (unique) is owned by user-service; verify + warn only, never create.
	mongoutil.WarnMissingIndexes(ctx, s.users, "account_1")

	// Backs SearchUsers, whose only non-regex predicate is siteId: no other service
	// declares a siteId-prefixed index on the shared users collection, so without
	// this both the count and the paged find scan every user document. account
	// trails so the unfiltered count is answered from the index alone (a q-filtered
	// one still fetches, since engName/chineseName aren't in the key).
	_, err := s.users.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "siteId", Value: 1}, {Key: "account", Value: 1}},
	})
	if err != nil {
		return fmt.Errorf("create users siteId_account index: %w", err)
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

	// Backs ListPermissionGrants's subjectAccount+permission lookup: equality
	// prefix (permission, subjectAccount) + sort suffix (recordedAt, _id) so
	// newest-first ordering comes free from the index (spec §3.6). Company-wide
	// — the ledger browse no longer filters by siteId.
	_, err = s.permGrants.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "permission", Value: 1},
			{Key: "subjectAccount", Value: 1},
			{Key: "recordedAt", Value: -1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		return fmt.Errorf("create permission_grants permission_subjectAccount_recordedAt_id index: %w", err)
	}

	// Backs the audit/BI browse (no subjectAccount equality, so index 1 above
	// doesn't apply). The _id tiebreaker mirrors the list sort {recordedAt:-1,_id:-1}
	// — without it every browse pays a blocking in-memory SORT, and recordedAt ties
	// are guaranteed because a multi-subject batch stamps one shared timestamp.
	_, err = s.permGrants.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "recordedAt", Value: -1}, {Key: "_id", Value: -1}},
	})
	if err != nil {
		return fmt.Errorf("create permission_grants recordedAt_id index: %w", err)
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

// fanoutProjection is the post-write read-back: exactly the fields
// fanoutUserAccount publishes. Never include services/password.
var fanoutProjection = bson.M{"_id": 1, "account": 1, "siteId": 1,
	"engName": 1, "chineseName": 1, "roles": 1, "active": 1}

// SearchUsers spans every site: the admin console lists cross-site replicas
// too (read-only there — mutations stay home-site-scoped).
func (s *storeMongo) SearchUsers(ctx context.Context, q string, page, limit int) ([]model.User, int64, error) {
	filter := bson.M{}
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

func (s *storeMongo) UpdateUser(ctx context.Context, siteID, account string, fields UserUpdate) (*model.User, error) {
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
		return nil, nil
	}

	filter := bson.M{"account": account, "siteId": siteID}

	// Deactivation no longer flows through UpdateUser — the handler routes
	// active=false to DeactivateAndRevoke instead so the user-flag flip
	// and session-purge run in one Mongo transaction. UpdateUser stays
	// non-transactional for the remaining patch fields (roles, names).
	res := s.users.FindOneAndUpdate(ctx, filter, bson.M{"$set": set},
		options.FindOneAndUpdate().SetReturnDocument(options.After).SetProjection(fanoutProjection))
	var u model.User
	if err := res.Decode(&u); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("update user: %w", err)
	}
	return &u, nil
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
// live token. Requires a replica set. Returns the post-write doc projected to
// the fanout fields.
func (s *storeMongo) DeactivateAndRevoke(ctx context.Context, siteID, account string) (*model.User, error) {
	filter := bson.M{"account": account, "siteId": siteID}

	var updated *model.User
	err := s.withTransaction(ctx, func(ctx context.Context) error {
		res := s.users.FindOneAndUpdate(ctx, filter, bson.M{"$set": bson.M{"active": false}},
			options.FindOneAndUpdate().SetReturnDocument(options.After).SetProjection(fanoutProjection))
		var u model.User
		if err := res.Decode(&u); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return ErrUserNotFound
			}
			return fmt.Errorf("deactivate user: %w", err)
		}
		sessions := s.users.Database().Collection(session.Collection)
		if _, err := sessions.DeleteMany(ctx, filter); err != nil {
			return fmt.Errorf("revoke sessions: %w", err)
		}
		updated = &u
		return nil
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
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
	return s.AppendAuditMany(ctx, []*AuditEntry{e})
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

// Full-row projection: every PermissionGrant field is needed by the ledger views;
// keep in lockstep with the struct.
var permissionGrantProjection = bson.M{
	"_id":              1,
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

// RecordPermissionChange appends the batch to the ledger and applies the derived state to
// the subjects' user documents under the per-key watermark guard, in one transaction
// (withTransaction + InsertMany + UpdateMany) — so a resend after a partial failure
// cannot produce duplicate ledger rows, and a stale write can never clobber a newer
// stored state (spec §4.4 note on step 11).
func (s *storeMongo) RecordPermissionChange(ctx context.Context, grants []*model.PermissionGrant, state model.PermissionState) error {
	if len(grants) == 0 {
		return nil
	}
	field, ok := model.PermissionFieldName(grants[0].Permission)
	if !ok {
		return fmt.Errorf("record permission change: unknown permission %q", grants[0].Permission)
	}
	docs := make([]any, len(grants))
	accounts := make([]string, len(grants))
	for i, g := range grants {
		docs[i] = g
		accounts[i] = g.SubjectAccount
	}
	path := "permissions." + field
	filter := bson.M{
		"account": bson.M{"$in": accounts},
		"$or": bson.A{
			bson.M{path + ".updatedAt": bson.M{"$exists": false}},
			bson.M{path + ".updatedAt": bson.M{"$lte": state.UpdatedAt}},
		},
	}
	return s.withTransaction(ctx, func(ctx context.Context) error {
		if _, err := s.permGrants.InsertMany(ctx, docs); err != nil {
			return fmt.Errorf("insert permission grants: %w", err)
		}
		// MatchedCount may be < len(accounts): a newer stored state (guard) or a user
		// doc missing locally is a no-op, not an error — same rule as the inbox apply.
		if _, err := s.users.UpdateMany(ctx, filter, bson.M{"$set": bson.M{path: state}}); err != nil {
			return fmt.Errorf("apply user permissions: %w", err)
		}
		return nil
	})
}

// GetUserPermissions returns the account's materialized permission snapshot; (nil, nil)
// when the user or snapshot does not exist. Site-unfiltered: subjects may be homed at
// any site.
func (s *storeMongo) GetUserPermissions(ctx context.Context, account string) (*model.UserPermissions, error) {
	var doc struct {
		Permissions *model.UserPermissions `bson:"permissions"`
	}
	err := s.users.FindOne(ctx, bson.M{"account": account},
		options.FindOne().SetProjection(bson.M{"permissions": 1})).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find user permissions: %w", err)
	}
	return doc.Permissions, nil
}

// GetUserPermissionsForAccounts returns the materialized snapshots for the given
// accounts in one read; accounts with no user doc are absent from the map.
func (s *storeMongo) GetUserPermissionsForAccounts(ctx context.Context, accounts []string) (map[string]*model.UserPermissions, error) {
	cur, err := s.users.Find(ctx, bson.M{"account": bson.M{"$in": accounts}},
		options.Find().SetProjection(bson.M{"account": 1, "permissions": 1}))
	if err != nil {
		return nil, fmt.Errorf("find user permissions: %w", err)
	}

	var rows []struct {
		Account     string                 `bson:"account"`
		Permissions *model.UserPermissions `bson:"permissions"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode user permissions: %w", err)
	}

	out := make(map[string]*model.UserPermissions, len(rows))
	for _, row := range rows {
		out[row.Account] = row.Permissions
	}
	return out, nil
}

// ListPermissionGrants returns the ledger newest-first (recordedAt desc, _id
// desc), company-wide (site-unfiltered — subjects may be homed at any site).
// subjectAccount == "" means all subjects; permission == "" means all
// permissions — the two filters combine independently (spec §4.6). Omitting
// either or both breaks the equality prefix on index 1, so this path falls
// back to index 2 (recordedAt, _id) plus a residual filter, or — when both
// are omitted — index 2 alone with no residual filter; accepted per spec.
func (s *storeMongo) ListPermissionGrants(ctx context.Context, subjectAccount string, permission model.PermissionKey, page, limit int) ([]model.PermissionGrant, int64, error) {
	filter := bson.M{}
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

// accountStateProjection contains only the fields FindAccountStates needs to
// derive IsActive() — never the rest of the user document.
var accountStateProjection = bson.M{"account": 1, "active": 1}

// FindAccountStates returns account -> IsActive() for the accounts that exist
// company-wide (the local users collection covers every site's users; subjects
// may be homed anywhere) — accounts not present in the map do not exist. One
// query for the whole batch (spec §4.4 step 10), rather than N lookups.
func (s *storeMongo) FindAccountStates(ctx context.Context, accounts []string) (map[string]bool, error) {
	cur, err := s.users.Find(ctx,
		bson.M{"account": bson.M{"$in": accounts}},
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
