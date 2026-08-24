package subject

import "fmt"

// DROplog builds the subject for one disaster-recovery CDC event shipped from an origin site
// to the shared backup: chat.dr.oplog.{siteID}.{collection}.{op}. siteID is the origin site
// whose operational Mongo was tailed; collection is the new-stack collection name (e.g. rooms);
// op is insert|update|replace|delete.
func DROplog(siteID, collection, op string) string {
	return fmt.Sprintf("chat.dr.oplog.%s.%s.%s", siteID, collection, op)
}

// DROplogWildcard matches every DR oplog event for an origin site — the DR-OPLOG-{siteID}
// stream's subjects, and the pattern the backup sources over the supercluster gateway.
func DROplogWildcard(siteID string) string {
	return fmt.Sprintf("chat.dr.oplog.%s.>", siteID)
}
