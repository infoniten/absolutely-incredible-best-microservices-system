package filter

type FieldUsage struct {
	indexKeys map[string]struct{}
	needsData bool
}

func NewFieldUsage() *FieldUsage {
	return &FieldUsage{indexKeys: make(map[string]struct{})}
}

func (u *FieldUsage) Apply(resolution FieldResolution) {
	if resolution.IndexKey != "" {
		u.indexKeys[resolution.IndexKey] = struct{}{}
	}
	if resolution.NeedsData {
		u.needsData = true
	}
}

func (u *FieldUsage) UsesIndex(source IndexSource) bool {
	if u == nil {
		return false
	}
	_, ok := u.indexKeys[source.Source]
	return ok
}

func (u *FieldUsage) NeedsData() bool {
	if u == nil {
		return false
	}
	return u.needsData
}

func (u *FieldUsage) RequireData() {
	if u != nil {
		u.needsData = true
	}
}
