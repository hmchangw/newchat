package mongorepo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/user-service/models"
)

const (
	appsCollection = "apps"
	// fabDomainMappingCollection keeps its legacy name for verbatim migration.
	fabDomainMappingCollection = "fab_domain_mapping"
)

// AppRepo is the Mongo implementation of service.AppRepository.
type AppRepo struct {
	apps *mongoutil.Collection[model.App]
	// items views the same collection decoded as AppListItem ($addFields isSubscribed).
	items *mongoutil.Collection[models.AppListItem]
	// categories is the fab-domain → site mapping backing apps.categories.
	categories *mongoutil.Collection[appCategoryDoc]
}

// appCategoryDoc decodes a fab_domain_mapping row. _id is a native Mongo
// ObjectID (legacy data), so it can't decode into a string field — exposed as hex.
// The collection is homogeneous ObjectID-keyed; a non-ObjectID _id would fail the
// whole decode (cursor.All), so a mixed collection would need a $type filter.
type appCategoryDoc struct {
	ID     bson.ObjectID `bson:"_id"`
	Name   string        `bson:"name"`
	SiteID string        `bson:"siteId"`
}

// NewAppRepo builds an AppRepo over db.
func NewAppRepo(db *mongo.Database) *AppRepo {
	col := db.Collection(appsCollection)
	return &AppRepo{
		apps:       mongoutil.NewCollection[model.App](col),
		items:      mongoutil.NewCollection[models.AppListItem](col),
		categories: mongoutil.NewCollection[appCategoryDoc](db.Collection(fabDomainMappingCollection)),
	}
}

// EnsureIndexes creates the assistant.name index (shared name with room-service
// to avoid IndexOptionsConflict) and the fab_domain_mapping {name, _id} index —
// compound so it can back the {name:1, _id:1} sort, which a lone {name:1} cannot.
func (r *AppRepo) EnsureIndexes(ctx context.Context) error {
	appsIndex := mongo.IndexModel{
		Keys:    bson.D{{Key: "assistant.name", Value: 1}},
		Options: options.Index().SetName("assistant_name_idx"),
	}
	if _, err := r.apps.Raw().Indexes().CreateOne(ctx, appsIndex); err != nil {
		return fmt.Errorf("ensure apps index: %w", err)
	}
	categoryIndex := mongo.IndexModel{
		Keys:    bson.D{{Key: "name", Value: 1}, {Key: "_id", Value: 1}},
		Options: options.Index().SetName("fab_domain_name_id_idx"),
	}
	if _, err := r.categories.Raw().Indexes().CreateOne(ctx, categoryIndex); err != nil {
		return fmt.Errorf("ensure fab_domain_mapping index: %w", err)
	}
	return nil
}

// GetApp returns the app by id, or (nil, nil) when none matches.
func (r *AppRepo) GetApp(ctx context.Context, appID string) (*model.App, error) {
	return r.apps.FindByID(ctx, appID)
}

// ListApps returns a name-sorted page of apps with isSubscribed per user, plus a
// hasMore flag (the query over-fetches by one).
func (r *AppRepo) ListApps(ctx context.Context, account string, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[models.AppListItem], error) {
	pipeline := bson.A{
		bson.M{"$lookup": bson.M{
			"from": subscriptionsCollection,
			"let":  bson.M{"botName": "$assistant.name"},
			"pipeline": bson.A{bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
				// $literal so a $-prefixed account isn't read as a field path.
				bson.M{"$eq": bson.A{"$u.account", bson.M{"$literal": account}}},
				bson.M{"$eq": bson.A{"$name", "$$botName"}},
				bson.M{"$eq": bson.A{"$roomType", "botDM"}},
				bson.M{"$eq": bson.A{"$isSubscribed", true}},
			}}}}},
			"as": "sub",
		}},
		bson.M{"$addFields": bson.M{"isSubscribed": bson.M{"$gt": bson.A{bson.M{"$size": "$sub"}, 0}}}},
		bson.M{"$project": bson.M{"sub": 0}},
		bson.M{"$sort": bson.M{"name": 1}},
	}
	out, err := r.items.AggregatePagedHasMore(ctx, pipeline, page)
	if err != nil {
		return mongoutil.OffsetPageHasMore[models.AppListItem]{}, fmt.Errorf("aggregate apps page: %w", err)
	}
	return out, nil
}

// ListAppCategories returns all mappings sorted by name; the collection is small, so no pagination.
// The _id tiebreaker makes ordering deterministic when two rows share a name.
func (r *AppRepo) ListAppCategories(ctx context.Context) ([]models.AppCategory, error) {
	docs, err := r.categories.FindMany(ctx, bson.M{},
		mongoutil.WithSort(bson.D{{Key: "name", Value: 1}, {Key: "_id", Value: 1}}),
		mongoutil.WithProjection(bson.D{
			{Key: "_id", Value: 1},
			{Key: "name", Value: 1},
			{Key: "siteId", Value: 1},
		}))
	if err != nil {
		return nil, fmt.Errorf("find app category mappings: %w", err)
	}
	cats := make([]models.AppCategory, len(docs))
	for i, d := range docs {
		cats[i] = models.AppCategory{ID: d.ID.Hex(), Name: d.Name, SiteID: d.SiteID}
	}
	return cats, nil
}

// GetAppsByAssistants maps bot account (assistant.name) → the full app document for the given accounts.
func (r *AppRepo) GetAppsByAssistants(ctx context.Context, botAccounts []string) (map[string]*model.App, error) {
	apps, err := r.apps.FindMany(ctx, bson.M{"assistant.name": bson.M{"$in": botAccounts}})
	if err != nil {
		return nil, fmt.Errorf("find apps by assistant names: %w", err)
	}
	out := make(map[string]*model.App, len(apps))
	for i := range apps {
		if apps[i].Assistant != nil {
			app := apps[i]
			out[app.Assistant.Name] = &app
		}
	}
	return out, nil
}
