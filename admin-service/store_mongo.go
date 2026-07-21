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
}

func newStoreMongo(db *mongo.Database) *storeMongo {
	return &storeMongo{
		users:      db.Collection("users"),
		adminAudit: db.Collection("admin_audit"),
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
	"deactivated":           1,
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
	"deactivated":           1,
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
	if fields.Deactivated != nil {
		set["deactivated"] = *fields.Deactivated
	}
	if len(set) == 0 {
		return nil
	}

	filter := bson.M{"account": account, "siteId": siteID}

	// Deactivation no longer flows through UpdateUser — the handler routes
	// Deactivated=true to DeactivateAndRevoke instead so the user-flag flip
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

// DeactivateAndRevoke atomically sets deactivated=true on the user and
// deletes every session for the account, so a disabled account can't keep a
// live token. Requires a replica set.
func (s *storeMongo) DeactivateAndRevoke(ctx context.Context, siteID, account string) error {
	filter := bson.M{"account": account, "siteId": siteID}

	return s.withTransaction(ctx, func(ctx context.Context) error {
		result, err := s.users.UpdateOne(ctx, filter, bson.M{"$set": bson.M{"deactivated": true}})
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

func (s *storeMongo) Ping(ctx context.Context) error {
	return s.users.Database().Client().Ping(ctx, nil)
}
