package filter

import (
	"strconv"
	"strings"

	"github.com/quantara/search-service/internal/domain"
	"github.com/quantara/search-service/internal/registry"
)

type indexSourceDefinition struct {
	source           string
	table            string
	columns          map[string]domain.FieldColumn
	sourceNormalized string
}

type IndexSourceFactory struct {
	cache map[*domain.ObjectClassInfo][]indexSourceDefinition
}

func NewIndexSourceFactory(cfg *domain.SearchDomainConfig, classRegistry *registry.ObjectClassRegistry) *IndexSourceFactory {
	factory := &IndexSourceFactory{cache: make(map[*domain.ObjectClassInfo][]indexSourceDefinition)}
	if cfg == nil || classRegistry == nil {
		return factory
	}

	for actualClassName, indexSources := range cfg.IndexSources {
		actualClass := classRegistry.ByName(actualClassName)
		if actualClass == nil {
			continue
		}

		definitions := make([]indexSourceDefinition, 0, len(indexSources))
		for _, sourceCfg := range indexSources {
			columnsClass := classRegistry.ByName(sourceCfg.ColumnsRef)
			columns := map[string]domain.FieldColumn{}
			if columnsClass != nil {
				for key, value := range columnsClass.IndexColumns {
					columns[key] = value
				}
			}

			definitions = append(definitions, indexSourceDefinition{
				source:           sourceCfg.Source,
				table:            sourceCfg.Table,
				columns:          columns,
				sourceNormalized: normalizeSourceValue(sourceCfg.Source),
			})
		}

		factory.cache[actualClass] = definitions
	}

	return factory
}

func (f *IndexSourceFactory) Build(actualClass *domain.ObjectClassInfo) []IndexSource {
	if f == nil || actualClass == nil {
		return nil
	}

	definitions := f.cache[actualClass]
	if len(definitions) == 0 {
		return nil
	}

	result := make([]IndexSource, 0, len(definitions))
	for idx, definition := range definitions {
		result = append(result, IndexSource{
			Source:           definition.source,
			Table:            definition.table,
			Alias:            "i" + strconv.Itoa(idx),
			Columns:          definition.columns,
			SourceNormalized: definition.sourceNormalized,
		})
	}

	return result
}

func normalizeSourceValue(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
