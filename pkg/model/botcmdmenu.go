package model

// BotCmdMenu is a row in the bot_cmd_menu collection. Name matches an
// AppAssistant.Name (the bot account) and joins back to the owning App
// via that field. ActiveStatus gates whether the menu is currently
// exposed to clients.
type BotCmdMenu struct {
	ID           string     `json:"id"           bson:"_id"`
	Name         string     `json:"name"         bson:"name"`
	ActiveStatus bool       `json:"activeStatus" bson:"activeStatus"`
	CmdBlocks    []CmdBlock `json:"cmdBlocks,omitempty" bson:"cmdBlocks,omitempty"`
}

// CmdBlock is the recursive building block of a bot command menu. A
// block either renders directly (Text+ActionType+Payload), opens a
// modal (Modal), or groups nested blocks (Blocks). Fields are
// optional so the schema can evolve without breaking the wire
// contract.
type CmdBlock struct {
	Text        string     `json:"text,omitempty"        bson:"text,omitempty"`
	ActionType  string     `json:"actionType,omitempty"  bson:"actionType,omitempty"`
	Description string     `json:"description,omitempty" bson:"description,omitempty"`
	Payload     string     `json:"payload,omitempty"     bson:"payload,omitempty"`
	Modal       *CmdModal  `json:"modal,omitempty"       bson:"modal,omitempty"`
	Blocks      []CmdBlock `json:"blocks,omitempty"      bson:"blocks,omitempty"`
}

// CmdModal carries the slash-style command + param a modal-triggering
// block invokes. CmdModal does NOT nest its own blocks — recursive
// rendering happens via CmdBlock.Blocks on the enclosing CmdBlock.
type CmdModal struct {
	Command string `json:"command,omitempty" bson:"command,omitempty"`
	Param   string `json:"param,omitempty"   bson:"param,omitempty"`
}
