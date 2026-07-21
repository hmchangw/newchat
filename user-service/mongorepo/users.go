package mongorepo

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

const usersCollection = "users"

// UserRepo is the Mongo implementation of service.UserRepository.
type UserRepo struct {
	users *mongoutil.Collection[model.User]
}

// NewUserRepo builds a UserRepo over db.
func NewUserRepo(db *mongo.Database) *UserRepo {
	return &UserRepo{
		users: mongoutil.NewCollection[model.User](db.Collection(usersCollection)),
	}
}

// EnsureIndexes creates user indexes. The unique account index is shared with room-service; failure messages guide the operator to the same fix.
func (r *UserRepo) EnsureIndexes(ctx context.Context) error {
	_, err := r.users.Raw().Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "account", Value: 1}}, Options: options.Index().SetUnique(true),
	})
	if err == nil {
		return nil
	}
	// E11000: pre-existing duplicate accounts (populated env pre-rollout) — point operators at the one-time dedupe preflight.
	if mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("create user index: duplicate account values exist in the users collection — run the "+
			"one-time dedupe preflight (group users by account, resolve n>1) before starting this service: %w", err)
	}
	// A pre-existing non-unique account_1 conflicts (85 IndexOptionsConflict / 86 IndexKeySpecsConflict); Mongo won't upgrade it — the operator must drop it.
	if se := mongo.ServerError(nil); errors.As(err, &se) && (se.HasErrorCode(85) || se.HasErrorCode(86)) {
		return fmt.Errorf("create user index: a non-unique account_1 index already exists on the users collection — "+
			"drop the old non-unique account_1 index (db.users.dropIndex(\"account_1\")) before starting this service "+
			"so it can be recreated as unique: %w", err)
	}
	return fmt.Errorf("create user index: %w", err)
}

// activeUserFilter matches non-deactivated users. Missing `active` is treated as active ({$ne:false}); only explicit false excludes.
func activeUserFilter(account string) bson.M {
	return bson.M{"account": account, "active": bson.M{"$ne": false}}
}

// GetUserStatus returns the user for account (missing `active` counts as active),
// or (nil, nil). Projected to the UserStatusView fields; all others are zero-valued.
func (r *UserRepo) GetUserStatus(ctx context.Context, account string) (*model.User, error) {
	return r.users.FindOne(ctx, activeUserFilter(account),
		mongoutil.WithProjection(bson.M{
			"_id": 0, "account": 1, "statusText": 1, "statusIsShow": 1,
			"chineseName": 1, "engName": 1,
		}),
	)
}

// GetHRInfoByAccounts maps account → the counterpart's HR-directory record for DM
// sidebar/header rendering. hrInfo.name mirrors the chineseName field, matching the
// hrUser $lookup in GetDMSubscription. Accounts with no users doc are omitted.
func (r *UserRepo) GetHRInfoByAccounts(ctx context.Context, accounts []string) (map[string]*model.SubscriptionHRInfo, error) {
	type hrUser struct {
		Account     string `bson:"account"`
		ChineseName string `bson:"chineseName"`
		EngName     string `bson:"engName"`
	}
	col := mongoutil.NewCollection[hrUser](r.users.Raw())
	rows, err := col.FindMany(ctx,
		bson.M{"account": bson.M{"$in": accounts}},
		mongoutil.WithProjection(bson.M{"_id": 0, "account": 1, "chineseName": 1, "engName": 1}),
	)
	if err != nil {
		return nil, fmt.Errorf("find hr info by accounts: %w", err)
	}
	out := make(map[string]*model.SubscriptionHRInfo, len(rows))
	for i := range rows {
		out[rows[i].Account] = &model.SubscriptionHRInfo{
			Account: rows[i].Account,
			Name:    rows[i].ChineseName,
			EngName: rows[i].EngName,
		}
	}
	return out, nil
}

// GetUserSettings returns the user's stored settings sub-document (Settings is
// nil when never set); (nil, nil) when no active user matched.
func (r *UserRepo) GetUserSettings(ctx context.Context, account string) (*model.User, error) {
	return r.users.FindOne(ctx, activeUserFilter(account),
		mongoutil.WithProjection(bson.M{"_id": 0, "settings": 1}),
	)
}

// UpdateUserSettings $sets settings.<field> for each non-nil field in set —
// unsent fields keep their stored value or stay absent — and returns the
// updated user (Settings projected) in one round-trip via
// FindOneAndUpdate(After); returns (nil, nil) when no active user matched.
// The caller guarantees at least one field is non-nil (an empty $set errors).
func (r *UserRepo) UpdateUserSettings(ctx context.Context, account string, set *model.UserSettings) (*model.User, error) {
	fields := bson.M{}
	if set.FullWidth != nil {
		fields["settings.fullWidth"] = *set.FullWidth
	}
	if set.TranslateMessageInto != nil {
		fields["settings.translateMessageInto"] = *set.TranslateMessageInto
	}
	if set.ShowMessagePreviewInSidebarList != nil {
		fields["settings.showMessagePreviewInSidebarList"] = *set.ShowMessagePreviewInSidebarList
	}
	if set.MuteAllNotifications != nil {
		fields["settings.muteAllNotifications"] = *set.MuteAllNotifications
	}
	if set.ShowMessagesAndPreviewsInNotifications != nil {
		fields["settings.showMessagesAndPreviewsInNotifications"] = *set.ShowMessagesAndPreviewsInNotifications
	}
	if set.ShowNotificationsDuringCallsAndMeetings != nil {
		fields["settings.showNotificationsDuringCallsAndMeetings"] = *set.ShowNotificationsDuringCallsAndMeetings
	}
	if set.ScrollToBottomInChat != nil {
		fields["settings.scrollToBottomInChat"] = *set.ScrollToBottomInChat
	}
	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After).
		SetProjection(bson.M{"_id": 0, "settings": 1})
	res := r.users.Raw().FindOneAndUpdate(ctx, activeUserFilter(account), bson.M{"$set": fields}, opts)
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("update user settings: %w", err)
	}
	var u model.User
	if err := res.Decode(&u); err != nil {
		return nil, fmt.Errorf("decode updated user settings: %w", err)
	}
	return &u, nil
}

// GetUserRoles returns the active user's account + roles (other fields zero-valued) for platform-admin checks, or (nil, nil) when unmatched.
func (r *UserRepo) GetUserRoles(ctx context.Context, account string) (*model.User, error) {
	return r.users.FindOne(ctx, activeUserFilter(account),
		mongoutil.WithProjection(bson.M{"_id": 0, "account": 1, "roles": 1}),
	)
}

// SetUserStatus updates status fields (isShow only written when non-nil) and
// returns the updated user in one round-trip via FindOneAndUpdate(After),
// projected to the UserStatusView fields; returns (nil, nil) when no active user matched.
func (r *UserRepo) SetUserStatus(ctx context.Context, account, text string, isShow *bool) (*model.User, error) {
	set := bson.M{"statusText": text}
	if isShow != nil {
		set["statusIsShow"] = *isShow
	}
	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After).
		SetProjection(bson.M{"_id": 0, "account": 1, "statusText": 1, "statusIsShow": 1, "chineseName": 1, "engName": 1})
	res := r.users.Raw().FindOneAndUpdate(ctx, activeUserFilter(account), bson.M{"$set": set}, opts)
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("update user status: %w", err)
	}
	var u model.User
	if err := res.Decode(&u); err != nil {
		return nil, fmt.Errorf("decode updated user status: %w", err)
	}
	return &u, nil
}
