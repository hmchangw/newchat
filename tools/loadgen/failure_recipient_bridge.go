package main

import failuremodel "github.com/hmchangw/chat/tools/loadgen/internal/failure"

type recipientEvidenceResult = failuremodel.RecipientEvidenceResult
type recipientDeliveryRoute = failuremodel.RecipientDeliveryRoute

const (
	recipientDeliveryRouteUnknown    = failuremodel.RecipientDeliveryUnknown
	recipientDeliveryRouteRoomGlobal = failuremodel.RecipientDeliveryRoomGlobal
	recipientDeliveryRouteRoomLocal  = failuremodel.RecipientDeliveryRoomLocal
	recipientDeliveryRouteUser       = failuremodel.RecipientDeliveryUser
)

type recipientExpectationConfig = failuremodel.RecipientExpectationConfig
type recipientEvidence = failuremodel.RecipientEvidence

const (
	recipientEvidenceUntracked  = failuremodel.RecipientEvidenceUntracked
	recipientEvidenceExpected   = failuremodel.RecipientEvidenceExpected
	recipientEvidenceDuplicate  = failuremodel.RecipientEvidenceDuplicate
	recipientEvidenceUnexpected = failuremodel.RecipientEvidenceUnexpected
	recipientEvidenceMismatch   = failuremodel.RecipientEvidenceMismatch
)

func newRecipientEvidence(allowDuplicates bool) *recipientEvidence {
	return failuremodel.NewRecipientEvidence(allowDuplicates)
}
