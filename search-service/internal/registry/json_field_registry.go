package registry

import (
	"strings"

	"github.com/quantara/search-service/internal/domain"
)

// JsonFieldRegistry stores only fields explicitly declared in jsonFields per class.
type JsonFieldRegistry struct {
	declared map[*domain.ObjectClassInfo]map[string]domain.ColumnType
}

func NewJsonFieldRegistry(
	cfg *domain.SearchDomainConfig,
	objectClassRegistry *ObjectClassRegistry,
) *JsonFieldRegistry {
	registry := &JsonFieldRegistry{
		declared: make(map[*domain.ObjectClassInfo]map[string]domain.ColumnType),
	}

	if cfg == nil || objectClassRegistry == nil {
		return registry
	}

	for className, fields := range cfg.Fields {
		objectClass := objectClassRegistry.ByName(className)
		if objectClass == nil {
			continue
		}

		types := make(map[string]domain.ColumnType)
		for _, field := range fields.JSONFields {
			name := normalizeJSONField(field.Name)
			if name == "" {
				continue
			}

			columnType, err := domain.ParseColumnType(field.Type)
			if err != nil {
				continue
			}
			types[name] = columnType
		}

		registry.declared[objectClass] = types
	}

	return registry
}

func (r *JsonFieldRegistry) IsFieldDeclared(objectClass *domain.ObjectClassInfo, field string) bool {
	if r == nil || objectClass == nil {
		return false
	}
	_, ok := r.declared[objectClass][normalizeJSONField(field)]
	return ok
}

func (r *JsonFieldRegistry) DeclaredJSONFieldType(objectClass *domain.ObjectClassInfo, field string) *domain.ColumnType {
	if r == nil || objectClass == nil {
		return nil
	}
	columnType, ok := r.declared[objectClass][normalizeJSONField(field)]
	if !ok {
		return nil
	}
	return &columnType
}

func normalizeJSONField(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
