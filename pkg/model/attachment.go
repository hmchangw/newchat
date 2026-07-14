package model

import "github.com/hmchangw/chat/pkg/model/cassandra"

// Attachment and ImageDimensions are defined in pkg/model/cassandra so
// cassandra.Message can embed them without an import cycle. These aliases keep
// model.Attachment usable from services that already import pkg/model.
type (
	Attachment      = cassandra.Attachment
	ImageDimensions = cassandra.ImageDimensions
)
