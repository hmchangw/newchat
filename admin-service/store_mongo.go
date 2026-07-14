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
)

type storeMongo struct {
	users      *mongo.Collection
	sessions   *mongo.Collection
	adminAudit *mongo.Collection
}

func newStoreMongo(db *mongo.Database) *storeMongo {
	return &storeMongo{
		users:      db.Collection("users"),
		sessions:   db.Collection("sessions"),
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

	// Matches botplatform's existing "userId_1_issuedAt_1" index.
	_, err = s.sessions.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "userId", Value: 1}, {Key: "issuedAt", Value: 1}},
	})
	if err != nil {
		return fmt.Errorf("create sessions userId_issuedAt index: %w", err)
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

	// Deactivation must atomically revoke the account's sessions — a failed
	// revoke after the deactivate would leave live tokens for a disabled user.
	if fields.Deactivated != nil && *fields.Deactivated {
		return s.withTransaction(ctx, func(ctx context.Context) error {
			result, err := s.users.UpdateOne(ctx, filter, bson.M{"$set": set})
			if err != nil {
				return fmt.Errorf("update user: %w", err)
			}
			if result.MatchedCount == 0 {
				return ErrUserNotFound
			}
			if _, err := s.sessions.DeleteMany(ctx, filter); err != nil {
				return fmt.Errorf("revoke sessions: %w", err)
			}
			return nil
		})
	}

	result, err := s.users.UpdateOne(ctx, filter, bson.M{"$set": set})
	if err != nil {
		return fmt.Errorf("update user: %w", err)
	}
	if result.MatchedCount == 0 {
		return ErrUserNotFound
	}
	return nil
}

// UpdateUserPassword replaces the bcrypt hash and atomically revokes every
// session for the account, so a leaked old credential cannot keep a session
// alive after the reset. Requires a replica set (see withTransaction).
func (s *storeMongo) UpdateUserPassword(ctx context.Context, siteID, account, bcryptHash string, requireChange bool) error {
	filter := bson.M{"account": account, "siteId": siteID}
	return s.withTransaction(ctx, func(ctx context.Context) error {
		result, err := s.users.UpdateOne(ctx, filter,
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
		if _, err := s.sessions.DeleteMany(ctx, filter); err != nil {
			return fmt.Errorf("revoke sessions: %w", err)
		}
		return nil
	})
}

// sessionProjection returns all session fields.
var sessionProjection = bson.M{
	"_id":      1,
	"userId":   1,
	"account":  1,
	"siteId":   1,
	"roles":    1,
	"issuedAt": 1,
}

func (s *storeMongo) FindSessionByHash(ctx context.Context, hash string) (*Session, error) {
	var sess Session
	err := s.sessions.FindOne(ctx, bson.M{"_id": hash},
		options.FindOne().SetProjection(sessionProjection),
	).Decode(&sess)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("find session by hash: %w", err)
	}
	return &sess, nil
}

func (s *storeMongo) ListSessionsByAccount(ctx context.Context, siteID, account string) ([]Session, error) {
	cur, err := s.sessions.Find(ctx, bson.M{"siteId": siteID, "account": account},
		options.Find().
			SetProjection(sessionProjection).
			SetSort(bson.D{{Key: "issuedAt", Value: -1}}),
	)
	if err != nil {
		return nil, fmt.Errorf("list sessions: %w", err)
	}
	var sessions []Session
	if err := cur.All(ctx, &sessions); err != nil {
		return nil, fmt.Errorf("decode sessions: %w", err)
	}
	if sessions == nil {
		sessions = []Session{}
	}
	return sessions, nil
}

// DeleteSessionsByAccount revokes every session for the given account — the
// account-keyed variant used by the admin session-management endpoints.
func (s *storeMongo) DeleteSessionsByAccount(ctx context.Context, siteID, account string) (int64, error) {
	res, err := s.sessions.DeleteMany(ctx, bson.M{"siteId": siteID, "account": account})
	if err != nil {
		return 0, fmt.Errorf("delete sessions by account: %w", err)
	}
	return res.DeletedCount, nil
}

// DeleteSession revokes a single session scoped to the given site + account so
// an admin cannot revoke a session outside their site or belonging to a
// different account than the one queried.
func (s *storeMongo) DeleteSession(ctx context.Context, siteID, account, sessionID string) (int64, error) {
	res, err := s.sessions.DeleteOne(ctx, bson.M{"_id": sessionID, "siteId": siteID, "account": account})
	if err != nil {
		return 0, fmt.Errorf("delete session: %w", err)
	}
	return res.DeletedCount, nil
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
