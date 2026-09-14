package mongorepo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

const appsCollection = "apps"

type AppRepo struct {
	apps *mongoutil.Collection[model.App]
}

func NewAppRepo(db *mongo.Database) *AppRepo {
	return &AppRepo{
		apps: mongoutil.NewCollection[model.App](db.Collection(appsCollection)),
	}
}

// AppNameByAccount returns the app's display name for the bot account
// (assistant.name), or ("", nil) when no app matches.
func (r *AppRepo) AppNameByAccount(ctx context.Context, botAccount string) (string, error) {
	app, err := r.apps.FindOne(ctx, bson.M{"assistant.name": botAccount},
		mongoutil.WithProjection(bson.M{"name": 1, "_id": 0}))
	if err != nil {
		return "", err
	}
	if app == nil {
		return "", nil
	}
	return app.Name, nil
}

// AppNamesByAccounts returns app.name keyed by assistant.name for every apps
// document matching one of botAccounts, in a single read. Accounts with no app are
// absent from the map rather than an error. The existing apps (assistant.name)
// index covers the $in filter.
func (r *AppRepo) AppNamesByAccounts(ctx context.Context, botAccounts []string) (map[string]string, error) {
	if len(botAccounts) == 0 {
		return nil, nil
	}
	apps, err := r.apps.FindMany(ctx, bson.M{"assistant.name": bson.M{"$in": botAccounts}},
		mongoutil.WithProjection(bson.M{"name": 1, "assistant.name": 1, "_id": 0}))
	if err != nil {
		return nil, fmt.Errorf("find app names by accounts: %w", err)
	}
	names := make(map[string]string, len(apps))
	for i := range apps {
		// A document whose assistant did not project (or is absent) names nobody.
		if a := apps[i].Assistant; a != nil && a.Name != "" {
			names[a.Name] = apps[i].Name
		}
	}
	return names, nil
}
