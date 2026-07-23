package mongorepo

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

// ssoTokensCollection stores SSO token pairs (legacy field names kept).
// Migrated legacy token docs must be loaded into this collection.
const ssoTokensCollection = "sso_tokens"

// SSOTokenRepo is the Mongo store for service.SSOTokenRepository, keyed by username.
type SSOTokenRepo struct {
	col  *mongo.Collection
	docs *mongoutil.Collection[ssoTokenDoc]
}

// ssoTokenDoc is the repo-local read model; the legacy idTokenExp is a decimal-millis string, converted to int64 before crossing the store boundary.
type ssoTokenDoc struct {
	Username     string `bson:"username"`
	IDToken      string `bson:"idToken"`
	IDTokenExp   string `bson:"idTokenExp"`
	RefreshToken string `bson:"refreshToken"`
}

// NewSSOTokenRepo builds an SSOTokenRepo over db.
func NewSSOTokenRepo(db *mongo.Database) *SSOTokenRepo {
	col := db.Collection(ssoTokensCollection)
	return &SSOTokenRepo{col: col, docs: mongoutil.NewCollection[ssoTokenDoc](col)}
}

// EnsureIndexes creates the unique username index.
func (r *SSOTokenRepo) EnsureIndexes(ctx context.Context) error {
	_, err := r.col.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "username", Value: 1}}, Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("create sso token index: %w", err)
	}
	return nil
}

// GetByUsername returns the stored token pair for username, or (nil, nil).
func (r *SSOTokenRepo) GetByUsername(ctx context.Context, username string) (*model.SSOToken, error) {
	d, err := r.docs.FindOne(ctx, bson.M{"username": username},
		mongoutil.WithProjection(bson.M{
			"_id": 0, "username": 1, "idToken": 1, "idTokenExp": 1, "refreshToken": 1,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("find sso token: %w", err)
	}
	if d == nil {
		return nil, nil
	}
	expMs, _ := strconv.ParseInt(d.IDTokenExp, 10, 64) // non-numeric ⇒ 0 ⇒ reads as expired
	return &model.SSOToken{
		Username:     d.Username,
		IDToken:      d.IDToken,
		IDTokenExp:   expMs,
		RefreshToken: d.RefreshToken,
	}, nil
}

// Upsert stores the token pair for username (last-write-wins); idTokenExp persists as a decimal-millis string, new docs get an idgen _id, existing keep theirs.
func (r *SSOTokenRepo) Upsert(ctx context.Context, username, ssoToken string, ssoTokenExpMs int64, refreshToken string) error {
	update := bson.M{
		"$set": bson.M{
			"idToken":      ssoToken,
			"idTokenExp":   strconv.FormatInt(ssoTokenExpMs, 10),
			"refreshToken": refreshToken,
			"_updatedAt":   time.Now().UTC(),
		},
		"$setOnInsert": bson.M{"_id": idgen.GenerateID()},
	}
	_, err := r.col.UpdateOne(ctx, bson.M{"username": username}, update, options.UpdateOne().SetUpsert(true))
	// Two concurrent first-inserts can both take the insert branch; the loser's
	// E11000 means the doc now exists, so one retry lands as a plain update.
	if mongo.IsDuplicateKeyError(err) {
		_, err = r.col.UpdateOne(ctx, bson.M{"username": username}, update, options.UpdateOne().SetUpsert(true))
	}
	if err != nil {
		return fmt.Errorf("upsert sso token: %w", err)
	}
	return nil
}
