package registry

import (
	"fmt"
	"strings"

	"github.com/quantara/search-service/internal/domain"
)

type ObjectClassRegistry struct {
	byName   map[string]*domain.ObjectClassInfo
	bySource map[string]*domain.ObjectClassInfo
	all      []*domain.ObjectClassInfo
}

func NewObjectClassRegistry(cfg *domain.SearchDomainConfig) (*ObjectClassRegistry, error) {
	registry := &ObjectClassRegistry{
		byName:   make(map[string]*domain.ObjectClassInfo),
		bySource: make(map[string]*domain.ObjectClassInfo),
		all:      make([]*domain.ObjectClassInfo, 0),
	}

	if cfg == nil {
		return registry, nil
	}

	configsByName := make(map[string]domain.ClassConfig)
	for _, classCfg := range cfg.Classes {
		name := normalizeName(classCfg.Name)
		if name == "" {
			continue
		}
		configsByName[name] = classCfg
	}

	declaredMain := make(map[string]map[string]domain.FieldColumn)
	declaredIndex := make(map[string]map[string]domain.FieldColumn)
	for name := range configsByName {
		mainColumns, err := buildDeclaredColumns(cfg.Fields, name, true)
		if err != nil {
			return nil, err
		}
		indexColumns, err := buildDeclaredColumns(cfg.Fields, name, false)
		if err != nil {
			return nil, err
		}
		declaredMain[name] = mainColumns
		declaredIndex[name] = indexColumns
	}

	for name, classCfg := range configsByName {
		info := &domain.ObjectClassInfo{
			Name:                  name,
			SourceValue:           classCfg.SourceValue,
			SourceValueNormalized: normalizeSource(classCfg.SourceValue),
			IsAbstract:            classCfg.EffectiveAbstract(),
			RootType:              domain.ParseObjectRootType(classCfg.RootType),
			MainTable:             strings.TrimSpace(classCfg.MainTable),
			IndexTable:            strings.TrimSpace(classCfg.IndexTable),
			DataTable:             strings.TrimSpace(classCfg.DataTable),
			MainColumns:           declaredMain[name],
			IndexColumns:          declaredIndex[name],
		}

		registry.byName[name] = info
		if info.SourceValueNormalized != "" {
			registry.bySource[info.SourceValueNormalized] = info
		}
		registry.all = append(registry.all, info)
	}

	return registry, nil
}

func (r *ObjectClassRegistry) FromSourceValue(value string) *domain.ObjectClassInfo {
	if r == nil {
		return nil
	}
	return r.bySource[normalizeSource(value)]
}

func (r *ObjectClassRegistry) ByName(name string) *domain.ObjectClassInfo {
	if r == nil {
		return nil
	}
	return r.byName[normalizeName(name)]
}

func (r *ObjectClassRegistry) All() []*domain.ObjectClassInfo {
	if r == nil {
		return nil
	}
	result := make([]*domain.ObjectClassInfo, 0, len(r.all))
	result = append(result, r.all...)
	return result
}

func buildDeclaredColumns(source map[string]domain.FieldsConfig, className string, main bool) (map[string]domain.FieldColumn, error) {
	fieldsConfig, ok := findFieldsConfig(source, className)
	if !ok {
		return map[string]domain.FieldColumn{}, nil
	}

	columns := fieldsConfig.IndexFields
	if main {
		columns = fieldsConfig.MainFields
	}
	if len(columns) == 0 {
		return map[string]domain.FieldColumn{}, nil
	}

	result := make(map[string]domain.FieldColumn)
	for _, column := range columns {
		name := strings.TrimSpace(column.Name)
		dbColumn := strings.TrimSpace(column.DB)
		typeValue := strings.TrimSpace(column.Type)
		if name == "" || dbColumn == "" || typeValue == "" {
			continue
		}

		columnType, err := domain.ParseColumnType(typeValue)
		if err != nil {
			return nil, fmt.Errorf("failed to parse column type for class [%s], field [%s]: %w", className, name, err)
		}

		result[normalizeSource(name)] = domain.FieldColumn{
			SourceField: name,
			DBColumn:    dbColumn,
			Type:        columnType,
		}
	}

	return result, nil
}

func findFieldsConfig(source map[string]domain.FieldsConfig, className string) (domain.FieldsConfig, bool) {
	for key, value := range source {
		if normalizeName(key) == className {
			return value, true
		}
	}
	return domain.FieldsConfig{}, false
}

func normalizeName(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}

func normalizeSource(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
