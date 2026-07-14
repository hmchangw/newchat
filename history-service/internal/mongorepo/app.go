package mongorepo

import (
	"context"

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
