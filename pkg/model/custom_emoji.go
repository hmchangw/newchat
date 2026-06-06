package model

// CustomEmoji is a site-scoped custom reaction emoji; Shortcode is stored bare (no wrapping colons).
type CustomEmoji struct {
	ID        string `json:"id"        bson:"_id"`
	SiteID    string `json:"siteId"    bson:"siteId"`
	Shortcode string `json:"shortcode" bson:"shortcode"`
	ImageURL  string `json:"imageUrl"  bson:"imageUrl"`
	CreatedBy string `json:"createdBy" bson:"createdBy"`
	CreatedAt int64  `json:"createdAt" bson:"createdAt"`
}
