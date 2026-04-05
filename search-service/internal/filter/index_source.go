package filter

import "github.com/quantara/search-service/internal/domain"

type IndexSource struct {
	Source           string
	Table            string
	Alias            string
	Columns          map[string]domain.FieldColumn
	SourceNormalized string
}
