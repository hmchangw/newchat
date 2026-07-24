package main

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
)

type mongoStore struct {
	users         *mongo.Collection
	subscriptions *mongo.Collection
	avatars       *mongo.Collection
	customEmojis  *mongo.Collection
}

// Compile-time assertion that *mongoStore satisfies emojiStore. avatarStore
// is already referenced as a handler field type, so it doesn't need one.
var _ emojiStore = (*mongoStore)(nil)

func newMongoStore(db *mongo.Database) *mongoStore {
	return &mongoStore{
		users:         db.Collection("users"),
		subscriptions: db.Collection("subscriptions"),
		avatars:       db.Collection("avatars"),
		customEmojis:  db.Collection("custom_emojis"),
	}
}

func (s *mongoStore) EmployeeID(ctx context.Context, account string) (string, bool, error) {
	var u model.User
	err := s.users.FindOne(ctx, bson.M{"account": account},
		options.FindOne().SetProjection(bson.M{"employeeId": 1})).Decode(&u)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("find employeeId: %w", err)
	}
	if u.EmployeeID == "" {
		return "", false, nil
	}
	return u.EmployeeID, true, nil
}

func (s *mongoStore) BotSite(ctx context.Context, account string) (string, bool, error) {
	var u model.User
	err := s.users.FindOne(ctx, bson.M{"account": account},
		options.FindOne().SetProjection(bson.M{"siteId": 1})).Decode(&u)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("find bot site: %w", err)
	}
	if u.SiteID == "" {
		return "", false, nil
	}
	return u.SiteID, true, nil
}

func (s *mongoStore) RoomSite(ctx context.Context, roomID string) (string, model.RoomType, string, bool, error) {
	var sub model.Subscription
	err := s.subscriptions.FindOne(ctx, bson.M{"roomId": roomID},
		options.FindOne().SetProjection(bson.M{"siteId": 1, "roomType": 1, "name": 1})).Decode(&sub)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", "", "", false, nil
	}
	if err != nil {
		return "", "", "", false, fmt.Errorf("find room subscription: %w", err)
	}
	return sub.SiteID, sub.RoomType, sub.Name, true, nil
}

func (s *mongoStore) UserByAccount(ctx context.Context, account string) (*model.User, bool, error) {
	var u model.User
	err := s.users.FindOne(ctx, bson.M{"account": account},
		options.FindOne().SetProjection(bson.M{"_id": 1, "account": 1, "engName": 1, "chineseName": 1, "deactivated": 1})).Decode(&u)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("find user by account: %w", err)
	}
	return &u, true, nil
}

func (s *mongoStore) RoomMember(ctx context.Context, roomID, account string) (bool, error) {
	err := s.subscriptions.FindOne(ctx, bson.M{"roomId": roomID, "u.account": account},
		options.FindOne().SetProjection(bson.M{"_id": 1})).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("find room member: %w", err)
	}
	return true, nil
}

func (s *mongoStore) Avatar(ctx context.Context, subjectType model.AvatarSubjectType, subjectID string) (*model.Avatar, bool, error) {
	id := string(subjectType) + ":" + subjectID
	var av model.Avatar
	err := s.avatars.FindOne(ctx, bson.M{"_id": id}).Decode(&av)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("find avatar: %w", err)
	}
	return &av, true, nil
}

func (s *mongoStore) SetBotAvatar(ctx context.Context, av *model.Avatar) error {
	_, err := s.avatars.ReplaceOne(ctx, bson.M{"_id": av.ID}, av, options.Replace().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("upsert bot avatar: %w", err)
	}
	return nil
}

// EnsureEmojiIndexes creates the (siteId, shortcode) unique index; idempotent.
// media-service is the sole owner of the custom_emojis collection.
func (s *mongoStore) EnsureEmojiIndexes(ctx context.Context) error {
	_, err := s.customEmojis.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "siteId", Value: 1}, {Key: "shortcode", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("siteId_shortcode_unique"),
	})
	if err != nil {
		return fmt.Errorf("ensure custom_emojis indexes: %w", err)
	}
	return nil
}

func (s *mongoStore) EmojiDoc(ctx context.Context, siteID, shortcode string) (*model.CustomEmoji, bool, error) {
	var e model.CustomEmoji
	err := s.customEmojis.FindOne(ctx, bson.M{"siteId": siteID, "shortcode": shortcode},
		options.FindOne().SetProjection(bson.M{"minioKey": 1, "etag": 1})).Decode(&e)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("find custom emoji: %w", err)
	}
	return &e, true, nil
}

func (s *mongoStore) ListEmojis(ctx context.Context, siteID string) ([]model.CustomEmoji, error) {
	cur, err := s.customEmojis.Find(ctx, bson.M{"siteId": siteID},
		options.Find().
			SetProjection(bson.M{"shortcode": 1, "imageUrl": 1, "contentType": 1, "etag": 1, "updatedAt": 1}).
			SetSort(bson.D{{Key: "shortcode", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("list custom emojis: %w", err)
	}
	var out []model.CustomEmoji
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("decode custom emojis: %w", err)
	}
	return out, nil
}

func (s *mongoStore) UpsertEmoji(ctx context.Context, e *model.CustomEmoji) error {
	filter := bson.M{"siteId": e.SiteID, "shortcode": e.Shortcode}
	update := bson.M{
		"$set": bson.M{
			"imageUrl":    e.ImageURL,
			"updatedBy":   e.UpdatedBy,
			"updatedAt":   e.UpdatedAt,
			"minioKey":    e.MinioKey,
			"contentType": e.ContentType,
			"size":        e.Size,
			"etag":        e.ETag,
		},
		"$setOnInsert": bson.M{
			"_id":       e.ID,
			"siteId":    e.SiteID,
			"shortcode": e.Shortcode,
			"createdBy": e.CreatedBy,
			"createdAt": e.CreatedAt,
		},
	}
	opts := options.UpdateOne().SetUpsert(true)
	_, err := s.customEmojis.UpdateOne(ctx, filter, update, opts)
	if mongo.IsDuplicateKeyError(err) {
		// Two concurrent first-time creates raced the unique index; the retry
		// hits the now-existing doc as a plain update.
		_, err = s.customEmojis.UpdateOne(ctx, filter, update, opts)
	}
	if err != nil {
		return fmt.Errorf("upsert custom emoji: %w", err)
	}
	return nil
}

func (s *mongoStore) DeleteEmoji(ctx context.Context, siteID, shortcode string) (string, bool, error) {
	var e model.CustomEmoji
	err := s.customEmojis.FindOneAndDelete(ctx, bson.M{"siteId": siteID, "shortcode": shortcode},
		options.FindOneAndDelete().SetProjection(bson.M{"minioKey": 1})).Decode(&e)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("delete custom emoji: %w", err)
	}
	return e.MinioKey, true, nil
}
