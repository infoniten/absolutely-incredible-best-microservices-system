package registry

import (
	"github.com/quantara/search-service/internal/domain"
)

type ObjectClassHierarchyRegistry struct {
	parentsOrSelf        map[*domain.ObjectClassInfo]map[*domain.ObjectClassInfo]struct{}
	rootUnderBase        map[*domain.ObjectClassInfo]*domain.ObjectClassInfo
	actualClassesForRoot map[*domain.ObjectClassInfo][]*domain.ObjectClassInfo
}

func NewObjectClassHierarchyRegistry(cfg *domain.SearchDomainConfig, classRegistry *ObjectClassRegistry) *ObjectClassHierarchyRegistry {
	registry := &ObjectClassHierarchyRegistry{
		parentsOrSelf:        make(map[*domain.ObjectClassInfo]map[*domain.ObjectClassInfo]struct{}),
		rootUnderBase:        make(map[*domain.ObjectClassInfo]*domain.ObjectClassInfo),
		actualClassesForRoot: make(map[*domain.ObjectClassInfo][]*domain.ObjectClassInfo),
	}

	if cfg == nil || cfg.Hierarchy == nil || classRegistry == nil {
		return registry
	}

	for childName, parentNames := range cfg.Hierarchy.ParentsOrSelf {
		child := classRegistry.ByName(childName)
		if child == nil {
			continue
		}
		set := make(map[*domain.ObjectClassInfo]struct{})
		for _, parentName := range parentNames {
			parent := classRegistry.ByName(parentName)
			if parent != nil {
				set[parent] = struct{}{}
			}
		}
		registry.parentsOrSelf[child] = set
	}

	for childName, rootName := range cfg.Hierarchy.RootUnderBase {
		child := classRegistry.ByName(childName)
		root := classRegistry.ByName(rootName)
		if child != nil && root != nil {
			registry.rootUnderBase[child] = root
		}
	}

	for rootName, actualNames := range cfg.Hierarchy.ActualClassesForRoot {
		root := classRegistry.ByName(rootName)
		if root == nil {
			continue
		}
		actuals := make([]*domain.ObjectClassInfo, 0, len(actualNames))
		for _, name := range actualNames {
			actual := classRegistry.ByName(name)
			if actual != nil {
				actuals = append(actuals, actual)
			}
		}
		registry.actualClassesForRoot[root] = actuals
	}

	return registry
}

func (r *ObjectClassHierarchyRegistry) IsParentOrSelf(parent, child *domain.ObjectClassInfo) bool {
	if r == nil || parent == nil || child == nil {
		return false
	}
	set := r.parentsOrSelf[child]
	if set == nil {
		return false
	}
	_, ok := set[parent]
	return ok
}

func (r *ObjectClassHierarchyRegistry) ParentsOrSelf(objectClass *domain.ObjectClassInfo) []*domain.ObjectClassInfo {
	if r == nil || objectClass == nil {
		return nil
	}
	set := r.parentsOrSelf[objectClass]
	if set == nil {
		return nil
	}
	result := make([]*domain.ObjectClassInfo, 0, len(set))
	for class := range set {
		result = append(result, class)
	}
	return result
}

func (r *ObjectClassHierarchyRegistry) RootUnderBase(objectClass *domain.ObjectClassInfo) *domain.ObjectClassInfo {
	if r == nil || objectClass == nil {
		return objectClass
	}
	if root, ok := r.rootUnderBase[objectClass]; ok {
		return root
	}
	return objectClass
}

func (r *ObjectClassHierarchyRegistry) ActualClassesForRoot(rootClass *domain.ObjectClassInfo) []*domain.ObjectClassInfo {
	if r == nil || rootClass == nil {
		return nil
	}
	actual := r.actualClassesForRoot[rootClass]
	result := make([]*domain.ObjectClassInfo, 0, len(actual))
	result = append(result, actual...)
	return result
}
