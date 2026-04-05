package filter

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/quantara/search-service/internal/domain"
	"github.com/quantara/search-service/internal/registry"
)

var (
	apiFieldPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
)

type NotApplicableError struct {
	msg string
}

func (e *NotApplicableError) Error() string {
	return e.msg
}

func IsNotApplicable(err error) bool {
	_, ok := err.(*NotApplicableError)
	return ok
}

type FilterFieldRegistry struct {
	indexSources []IndexSource

	objectRootClass *domain.ObjectClassInfo
	actualClass     *domain.ObjectClassInfo
	rootClass       *domain.ObjectClassInfo

	jsonFieldEnabled bool

	objectClassRegistry *registry.ObjectClassRegistry
	hierarchyRegistry   *registry.ObjectClassHierarchyRegistry
	jsonFieldRegistry   *registry.JsonFieldRegistry
}

type FieldResolution struct {
	Field     FieldRef
	IndexKey  string
	NeedsData bool
}

func NewFilterFieldRegistry(
	objectRootClass *domain.ObjectClassInfo,
	actualClass *domain.ObjectClassInfo,
	indexSources []IndexSource,
	jsonFieldEnabled bool,
	objectClassRegistry *registry.ObjectClassRegistry,
	hierarchyRegistry *registry.ObjectClassHierarchyRegistry,
	jsonFieldRegistry *registry.JsonFieldRegistry,
) *FilterFieldRegistry {
	copiedSources := make([]IndexSource, 0, len(indexSources))
	copiedSources = append(copiedSources, indexSources...)

	return &FilterFieldRegistry{
		indexSources:        copiedSources,
		objectRootClass:     objectRootClass,
		actualClass:         actualClass,
		rootClass:           hierarchyRegistry.RootUnderBase(objectRootClass),
		jsonFieldEnabled:    jsonFieldEnabled,
		objectClassRegistry: objectClassRegistry,
		hierarchyRegistry:   hierarchyRegistry,
		jsonFieldRegistry:   jsonFieldRegistry,
	}
}

func (r *FilterFieldRegistry) Resolve(selector string) (FieldResolution, error) {
	normalized := strings.TrimSpace(selector)
	if normalized == "" {
		return FieldResolution{}, fmt.Errorf("selector cannot be empty")
	}

	firstDot := strings.Index(normalized, ".")
	if firstDot < 0 {
		return FieldResolution{}, fmt.Errorf("only source.field selectors are supported: [%s]", selector)
	}
	if strings.Index(normalized[firstDot+1:], ".") >= 0 {
		return FieldResolution{}, fmt.Errorf("nested selectors are not supported: [%s]", selector)
	}

	sourceKey := strings.TrimSpace(normalized[:firstDot])
	column := strings.TrimSpace(normalized[firstDot+1:])
	if sourceKey == "" || column == "" {
		return FieldResolution{}, fmt.Errorf("invalid selector: [%s]", selector)
	}

	return r.resolveWithSource(sourceKey, column)
}

func (r *FilterFieldRegistry) resolveWithSource(sourceKey, column string) (FieldResolution, error) {
	sourceClass := r.objectClassRegistry.FromSourceValue(sourceKey)
	if sourceClass == nil {
		return FieldResolution{}, fmt.Errorf("unknown source: [%s]", sourceKey)
	}

	if !r.hierarchyRegistry.IsParentOrSelf(r.objectRootClass, sourceClass) &&
		!r.hierarchyRegistry.IsParentOrSelf(sourceClass, r.objectRootClass) {
		return FieldResolution{}, fmt.Errorf("source does not belong to hierarchy: [%s], objectRootClass=[%s]",
			sourceKey, r.objectRootClass.SourceValue)
	}

	if !r.hierarchyRegistry.IsParentOrSelf(sourceClass, r.actualClass) {
		return FieldResolution{}, &NotApplicableError{msg: fmt.Sprintf(
			"source not applicable for class: [%s], actualClass=[%s]",
			sourceKey, r.actualClass.SourceValue,
		)}
	}

	// Mandatory gate: field must be explicitly declared in jsonFields for source class.
	if r.jsonFieldRegistry == nil || !r.jsonFieldRegistry.IsFieldDeclared(sourceClass, column) {
		return FieldResolution{}, fmt.Errorf("unknown field: [%s], source=[%s]", column, sourceClass.SourceValue)
	}

	// Then try fast paths from relational columns for current actualClass query.
	if r.hierarchyRegistry.IsParentOrSelf(sourceClass, r.rootClass) {
		mainColumn, ok := r.actualClass.MainColumns[normalizeFieldName(column)]
		if ok {
			if err := validateAPIField(column); err != nil {
				return FieldResolution{}, err
			}
			return r.mainField(mainColumn), nil
		}
	}

	if r.hierarchyRegistry.IsParentOrSelf(r.rootClass, sourceClass) {
		source := r.findIndexSource(sourceClass)
		if source != nil {
			if _, ok := source.Columns[normalizeFieldName(column)]; ok {
				if err := validateAPIField(column); err != nil {
					return FieldResolution{}, err
				}
				return r.indexField(*source, column), nil
			}
		}
	}

	if r.jsonFieldEnabled {
		if err := validateAPIField(column); err != nil {
			return FieldResolution{}, err
		}
		if !r.jsonFieldRegistry.IsFieldDeclared(sourceClass, column) {
			return FieldResolution{}, fmt.Errorf("unknown field: [%s], source=[%s]", column, sourceClass.SourceValue)
		}

		return r.dataField(column, r.jsonFieldRegistry.DeclaredJSONFieldType(sourceClass, column)), nil
	}

	return FieldResolution{}, fmt.Errorf("unknown field: [%s], source=[%s]", column, sourceClass.SourceValue)
}

func (r *FilterFieldRegistry) indexField(source IndexSource, column string) FieldResolution {
	indexColumn := source.Columns[normalizeFieldName(column)]
	columnType := indexColumn.Type
	return FieldResolution{
		Field: FieldRef{
			SQL:  source.Alias + "." + indexColumn.DBColumn,
			Type: &columnType,
			JSON: false,
		},
		IndexKey:  source.Source,
		NeedsData: false,
	}
}

func (r *FilterFieldRegistry) mainField(column domain.FieldColumn) FieldResolution {
	columnType := column.Type
	return FieldResolution{
		Field: FieldRef{
			SQL:  "m." + column.DBColumn,
			Type: &columnType,
			JSON: false,
		},
		NeedsData: false,
	}
}

func (r *FilterFieldRegistry) dataField(field string, fieldType *domain.ColumnType) FieldResolution {
	return FieldResolution{
		Field: FieldRef{
			SQL:  "d.content #>> '{" + field + "}'",
			Type: fieldType,
			JSON: true,
		},
		NeedsData: true,
	}
}

func (r *FilterFieldRegistry) findIndexSource(sourceClass *domain.ObjectClassInfo) *IndexSource {
	if sourceClass == nil {
		return nil
	}
	for _, source := range r.indexSources {
		if source.SourceNormalized == sourceClass.SourceValueNormalized {
			return &source
		}
	}
	return nil
}

func validateAPIField(value string) error {
	if !apiFieldPattern.MatchString(strings.TrimSpace(value)) {
		return fmt.Errorf("invalid field name: [%s]", value)
	}
	return nil
}

func normalizeFieldName(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
