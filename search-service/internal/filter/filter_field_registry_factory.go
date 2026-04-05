package filter

import (
	"github.com/quantara/search-service/internal/domain"
	"github.com/quantara/search-service/internal/registry"
)

type FilterFieldRegistryFactory struct {
	jsonFieldEnabled    bool
	objectClassRegistry *registry.ObjectClassRegistry
	hierarchyRegistry   *registry.ObjectClassHierarchyRegistry
	jsonFieldRegistry   *registry.JsonFieldRegistry
}

func NewFilterFieldRegistryFactory(
	jsonFieldEnabled bool,
	objectClassRegistry *registry.ObjectClassRegistry,
	hierarchyRegistry *registry.ObjectClassHierarchyRegistry,
	jsonFieldRegistry *registry.JsonFieldRegistry,
) *FilterFieldRegistryFactory {
	return &FilterFieldRegistryFactory{
		jsonFieldEnabled:    jsonFieldEnabled,
		objectClassRegistry: objectClassRegistry,
		hierarchyRegistry:   hierarchyRegistry,
		jsonFieldRegistry:   jsonFieldRegistry,
	}
}

func (f *FilterFieldRegistryFactory) Create(
	objectRootClass *domain.ObjectClassInfo,
	actualClass *domain.ObjectClassInfo,
	indexSources []IndexSource,
) *FilterFieldRegistry {
	return NewFilterFieldRegistry(
		objectRootClass,
		actualClass,
		indexSources,
		f.jsonFieldEnabled,
		f.objectClassRegistry,
		f.hierarchyRegistry,
		f.jsonFieldRegistry,
	)
}
