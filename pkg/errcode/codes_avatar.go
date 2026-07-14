package errcode

// Reasons emitted by media-service.
const (
	// AvatarWrongCluster signals an upload reached a cluster that does not own
	// the bot; the response message names the correct domain to retry against.
	AvatarWrongCluster Reason = "wrong_cluster"
)
