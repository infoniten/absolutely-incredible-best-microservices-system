package domain

// GlobalIDMapping represents the mapping between external trade ID and internal GlobalID
type GlobalIDMapping struct {
	ID               int64  `json:"id"`
	ExternalID       string `json:"externalId"`       // TRADENO from XML
	Source           string `json:"source"`           // MOEX
	SourceObjectType string `json:"sourceObjectType"` // FXSPOT
	GlobalID         int64  `json:"globalId"`
}

const (
	SourceMOEX           = "MOEX"
	SourceObjectTypeFXSPOT = "FXSPOT"
)
