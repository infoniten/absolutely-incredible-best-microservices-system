package domain

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"
)

type ColumnType string

const (
	ColumnTypeString   ColumnType = "STRING"
	ColumnTypeLong     ColumnType = "LONG"
	ColumnTypeInteger  ColumnType = "INTEGER"
	ColumnTypeDecimal  ColumnType = "DECIMAL"
	ColumnTypeDate     ColumnType = "DATE"
	ColumnTypeDateTime ColumnType = "DATETIME"
	ColumnTypeBoolean  ColumnType = "BOOLEAN"
)

func ParseColumnType(value string) (ColumnType, error) {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	switch ColumnType(normalized) {
	case ColumnTypeString, ColumnTypeLong, ColumnTypeInteger, ColumnTypeDecimal, ColumnTypeDate, ColumnTypeDateTime, ColumnTypeBoolean:
		return ColumnType(normalized), nil
	default:
		return "", fmt.Errorf("unknown column type: %s", value)
	}
}

func (c ColumnType) Parse(value string) (any, error) {
	switch c {
	case ColumnTypeString:
		return value, nil
	case ColumnTypeLong:
		if strings.TrimSpace(value) == "" {
			return nil, nil
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid long value: [%s]", value)
		}
		return parsed, nil
	case ColumnTypeInteger:
		if strings.TrimSpace(value) == "" {
			return nil, nil
		}
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return nil, fmt.Errorf("invalid integer value: [%s]", value)
		}
		return parsed, nil
	case ColumnTypeDecimal:
		if strings.TrimSpace(value) == "" {
			return nil, nil
		}
		if _, ok := new(big.Rat).SetString(value); !ok {
			return nil, fmt.Errorf("invalid decimal value: [%s]", value)
		}
		return value, nil
	case ColumnTypeDate:
		if strings.TrimSpace(value) == "" {
			return nil, nil
		}
		parsed, err := time.Parse("2006-01-02", value)
		if err != nil {
			return nil, fmt.Errorf("invalid date value: [%s], expected yyyy-MM-dd", value)
		}
		return parsed, nil
	case ColumnTypeDateTime:
		if strings.TrimSpace(value) == "" {
			return nil, nil
		}
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return nil, fmt.Errorf("invalid datetime value: [%s], expected ISO-8601 with offset", value)
		}
		return parsed, nil
	case ColumnTypeBoolean:
		trimmed := strings.TrimSpace(strings.ToLower(value))
		if trimmed == "" {
			return nil, nil
		}
		switch trimmed {
		case "true", "1":
			return true, nil
		case "false", "0":
			return false, nil
		default:
			return nil, fmt.Errorf("invalid boolean value: [%s]", value)
		}
	default:
		return value, nil
	}
}

type DraftStatus string

const (
	DraftStatusConfirmed DraftStatus = "CONFIRMED"
	DraftStatusDraft     DraftStatus = "DRAFT"
)

func ParseDraftStatus(value string) (DraftStatus, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", nil
	}
	normalized := strings.ToUpper(trimmed)
	switch DraftStatus(normalized) {
	case DraftStatusConfirmed, DraftStatusDraft:
		return DraftStatus(normalized), nil
	default:
		return "", fmt.Errorf("invalid draftStatus")
	}
}

type ObjectRootType string

const (
	ObjectRootTypeDraftableDateBoundedEntity ObjectRootType = "DRAFTABLE_DATE_BOUNDED_ENTITY"
	ObjectRootTypeRevisionedEntity           ObjectRootType = "REVISIONED_ENTITY"
	ObjectRootTypeEmbeddedEntity             ObjectRootType = "EMBEDDED_ENTITY"
	ObjectRootTypeNone                       ObjectRootType = "NONE"
)

func ParseObjectRootType(value string) ObjectRootType {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	switch ObjectRootType(normalized) {
	case ObjectRootTypeDraftableDateBoundedEntity, ObjectRootTypeRevisionedEntity, ObjectRootTypeEmbeddedEntity:
		return ObjectRootType(normalized)
	default:
		return ObjectRootTypeNone
	}
}

type FieldColumn struct {
	SourceField string
	DBColumn    string
	Type        ColumnType
}

type ObjectClassInfo struct {
	Name                  string
	SourceValue           string
	SourceValueNormalized string
	IsAbstract            bool
	RootType              ObjectRootType
	MainTable             string
	IndexTable            string
	DataTable             string
	MainColumns           map[string]FieldColumn
	IndexColumns          map[string]FieldColumn
}

type MainRecord struct {
	ID          int64
	ObjectClass string
}

type SearchDomainConfig struct {
	Classes      []ClassConfig                  `json:"classes"`
	Fields       map[string]FieldsConfig        `json:"fields"`
	Hierarchy    *HierarchyConfig               `json:"hierarchy"`
	IndexSources map[string][]IndexSourceConfig `json:"indexSources"`
}

type ClassConfig struct {
	Name        string `json:"name"`
	SourceValue string `json:"sourceValue"`
	RootType    string `json:"rootType"`
	MainTable   string `json:"mainTable"`
	IndexTable  string `json:"indexTable"`
	DataTable   string `json:"dataTable"`
	IsAbstract  bool   `json:"isAbstract"`
	Abstract    bool   `json:"abstract"`
}

func (c ClassConfig) EffectiveAbstract() bool {
	return c.IsAbstract || c.Abstract
}

type FieldsConfig struct {
	MainFields  []FieldConfig `json:"mainFields"`
	IndexFields []FieldConfig `json:"indexFields"`
	JSONFields  []FieldConfig `json:"jsonFields"`
}

type FieldConfig struct {
	Name string `json:"name"`
	DB   string `json:"db"`
	Type string `json:"type"`
}

type HierarchyConfig struct {
	ParentsOrSelf        map[string][]string `json:"parentsOrSelf"`
	RootUnderBase        map[string]string   `json:"rootUnderBase"`
	ActualClassesForRoot map[string][]string `json:"actualClassesForRoot"`
}

type IndexSourceConfig struct {
	Source     string `json:"source"`
	Table      string `json:"table"`
	ColumnsRef string `json:"columnsRef"`
}

type DomainConfigInfo struct {
	Source   string              `json:"source"`
	Location string              `json:"location"`
	Hash     string              `json:"hash"`
	LoadedAt time.Time           `json:"loadedAt"`
	Config   *SearchDomainConfig `json:"config"`
}
