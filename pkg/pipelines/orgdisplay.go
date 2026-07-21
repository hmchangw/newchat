package pipelines

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/orgdisplay"
)

// OrgDisplayUsers returns the dept/sect display rows feeding orgdisplay.Build, shared by
// room-service and room-worker so query+projection can't drift. The $or+$in is index-backed.
func OrgDisplayUsers(ctx context.Context, users *mongo.Collection, orgIDs []string) ([]orgdisplay.User, error) {
	if len(orgIDs) == 0 {
		return nil, nil
	}
	cursor, err := users.Find(ctx,
		bson.M{"$or": []bson.M{
			{"deptId": bson.M{"$in": orgIDs}},
			{"sectId": bson.M{"$in": orgIDs}},
		}},
		options.Find().SetProjection(bson.M{
			"_id":             0,
			"deptId":          1,
			"sectId":          1,
			"deptName":        1,
			"deptTCName":      1,
			"sectName":        1,
			"sectTCName":      1,
			"deptDescription": 1,
			"sectDescription": 1,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("find org display users: %w", err)
	}
	defer cursor.Close(ctx)

	var rows []orgdisplay.User
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode org display users: %w", err)
	}
	return rows, nil
}
