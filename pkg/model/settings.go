package model

import (
	"encoding/json"
	"time"
)

type UserSettings struct {
	Account   string          `json:"account"   bson:"account"`
	SiteID    string          `json:"siteId"    bson:"siteId"`
	Data      json.RawMessage `json:"data"      bson:"data"`
	Version   int64           `json:"version"   bson:"version"`
	UpdatedAt time.Time       `json:"updatedAt" bson:"updatedAt"`
}
