package filter

import (
	"fmt"
	"strings"

	"github.com/quantara/search-service/internal/dbschema"
)

type JoinPlan struct {
	Joins []string
}

type JoinPlanner struct {
	defaultSchema string
}

func NewJoinPlanner(defaultSchema string) *JoinPlanner {
	return &JoinPlanner{defaultSchema: strings.TrimSpace(defaultSchema)}
}

func (p *JoinPlanner) Plan(dataTable string, indexSources []IndexSource, usage *FieldUsage) (JoinPlan, error) {
	joins := make([]string, 0)
	for _, source := range indexSources {
		if usage.UsesIndex(source) {
			qualified, err := dbschema.QualifyTable(source.Table, p.defaultSchema)
			if err != nil {
				return JoinPlan{}, err
			}
			joins = append(joins, fmt.Sprintf("JOIN %s %s ON %s.id = m.id", qualified, source.Alias, source.Alias))
		}
	}

	if usage.NeedsData() {
		qualified, err := dbschema.QualifyTable(dataTable, p.defaultSchema)
		if err != nil {
			return JoinPlan{}, err
		}
		joins = append(joins, fmt.Sprintf("JOIN %s d ON d.id = m.id", qualified))
	}

	return JoinPlan{Joins: joins}, nil
}
