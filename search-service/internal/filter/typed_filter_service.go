package filter

import (
	"fmt"
	"strings"
	"time"

	"github.com/quantara/search-service/internal/dbschema"
	"github.com/quantara/search-service/internal/domain"
	pb "github.com/quantara/search-service/proto"
)

var maxActualTo = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)

type FilterQuery struct {
	SQL    string
	Params []any
}

func (q FilterQuery) IsEmpty() bool {
	return strings.TrimSpace(q.SQL) == ""
}

type sortField struct {
	selector  string
	ascending bool
}

type subquery struct {
	sql    string
	params []any
}

type TypedFilterService struct {
	sqlBuilder           *TypedFilterSQLBuilder
	indexSourceFactory   *IndexSourceFactory
	joinPlanner          *JoinPlanner
	fieldRegistryFactory *FilterFieldRegistryFactory
	defaultSchema        string
}

func NewTypedFilterService(
	sqlBuilder *TypedFilterSQLBuilder,
	indexSourceFactory *IndexSourceFactory,
	joinPlanner *JoinPlanner,
	fieldRegistryFactory *FilterFieldRegistryFactory,
	defaultSchema string,
) *TypedFilterService {
	return &TypedFilterService{
		sqlBuilder:           sqlBuilder,
		indexSourceFactory:   indexSourceFactory,
		joinPlanner:          joinPlanner,
		fieldRegistryFactory: fieldRegistryFactory,
		defaultSchema:        strings.TrimSpace(defaultSchema),
	}
}

func (s *TypedFilterService) BuildQuery(
	objectRootClass *domain.ObjectClassInfo,
	actualClasses []*domain.ObjectClassInfo,
	isDraft bool,
	actualDate *time.Time,
	historyInstant time.Time,
	filterExpr *pb.FilterExpr,
	sort []*pb.SortField,
	limit *int,
	offset *int,
	useLatestActualTo bool,
) (FilterQuery, error) {
	if len(actualClasses) == 0 {
		return FilterQuery{}, nil
	}
	if limit != nil && *limit < 0 {
		return FilterQuery{}, fmt.Errorf("limit must be >= 0")
	}
	if offset != nil && *offset < 0 {
		return FilterQuery{}, fmt.Errorf("offset must be >= 0")
	}
	if objectRootClass == nil {
		return FilterQuery{}, fmt.Errorf("objectRootClass does not support filtering")
	}
	if strings.TrimSpace(objectRootClass.MainTable) == "" {
		return FilterQuery{}, fmt.Errorf("objectRootClass does not support filtering: [%s]", objectRootClass.SourceValue)
	}
	mainTable, err := dbschema.QualifyTable(objectRootClass.MainTable, s.defaultSchema)
	if err != nil {
		return FilterQuery{}, err
	}

	sortFields, err := parseSort(sort)
	if err != nil {
		return FilterQuery{}, err
	}

	subqueries := make([]string, 0, len(actualClasses))
	params := make([]any, 0)
	for _, actualClass := range actualClasses {
		sub, err := s.prepareSubquery(
			mainTable,
			objectRootClass,
			actualClass,
			objectRootClass.RootType,
			isDraft,
			actualDate,
			historyInstant,
			filterExpr,
			sortFields,
			useLatestActualTo,
		)
		if err != nil {
			if IsNotApplicable(err) {
				continue
			}
			return FilterQuery{}, err
		}
		if sub == nil {
			continue
		}
		subqueries = append(subqueries, sub.sql)
		params = append(params, sub.params...)
	}

	if len(subqueries) == 0 {
		return FilterQuery{}, nil
	}

	unionSQL := subqueries[0]
	if len(subqueries) > 1 {
		unionSQL = strings.Join(subqueries, " UNION ALL ")
	}

	sql := "SELECT content FROM (" + unionSQL + ") t"
	sql = applySortLimitOffset(sql, sortFields, limit, offset, &params)
	return FilterQuery{SQL: sql, Params: params}, nil
}

func (s *TypedFilterService) prepareSubquery(
	mainTable string,
	objectRootClass *domain.ObjectClassInfo,
	actualClass *domain.ObjectClassInfo,
	rootType domain.ObjectRootType,
	isDraft bool,
	actualDate *time.Time,
	historyInstant time.Time,
	filterExpr *pb.FilterExpr,
	sortFields []sortField,
	useLatestActualTo bool,
) (*subquery, error) {
	indexSources := s.indexSourceFactory.Build(actualClass)
	usage := NewFieldUsage()
	fieldRegistry := s.fieldRegistryFactory.Create(
		objectRootClass,
		actualClass,
		indexSources,
	)

	resolver := func(selector string) (FieldRef, error) {
		resolution, err := fieldRegistry.Resolve(selector)
		if err != nil {
			return FieldRef{}, err
		}
		usage.Apply(resolution)
		return resolution.Field, nil
	}

	predicate, err := s.sqlBuilder.ToPredicate(filterExpr, resolver)
	if err != nil {
		return nil, err
	}

	sortExpressions, err := buildSortExpressions(sortFields, fieldRegistry, usage)
	if err != nil {
		return nil, err
	}

	usage.RequireData()
	dataTable := strings.TrimSpace(actualClass.DataTable)
	if usage.NeedsData() && dataTable == "" {
		return nil, nil
	}

	joinPlan, err := s.joinPlanner.Plan(dataTable, indexSources, usage)
	if err != nil {
		return nil, err
	}
	result, err := buildSubquerySQL(
		mainTable,
		actualClass,
		sortExpressions,
		joinPlan,
		rootType,
		isDraft,
		actualDate,
		historyInstant,
		useLatestActualTo,
		predicate,
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func buildSubquerySQL(
	mainTable string,
	actualClass *domain.ObjectClassInfo,
	sortExpressions []string,
	joinPlan JoinPlan,
	rootType domain.ObjectRootType,
	isDraft bool,
	actualDate *time.Time,
	historyInstant time.Time,
	useLatestActualTo bool,
	predicate SqlPredicate,
) (*subquery, error) {
	var sql strings.Builder
	sql.WriteString("SELECT d.content AS content")
	for i, expression := range sortExpressions {
		sql.WriteString(", ")
		sql.WriteString(expression)
		sql.WriteString(" AS sort_")
		sql.WriteString(fmt.Sprintf("%d", i))
	}

	sql.WriteString(" FROM ")
	sql.WriteString(mainTable)
	sql.WriteString(" m ")

	for _, join := range joinPlan.Joins {
		sql.WriteString(join)
		sql.WriteString(" ")
	}

	sql.WriteString("WHERE lower(m.object_class) = ? ")
	params := []any{actualClass.SourceValueNormalized}

	switch rootType {
	case domain.ObjectRootTypeDraftableDateBoundedEntity:
		sql.WriteString("AND m.draft_status = ? ")
		params = append(params, isDraft)

		if useLatestActualTo {
			sql.WriteString("AND m.actual_to = ? ")
			sql.WriteString("AND ? >= m.saved_at AND ? < m.closed_at ")
			params = append(params, maxActualTo)
			params = append(params, historyInstant, historyInstant)
		} else {
			if actualDate == nil {
				return nil, fmt.Errorf("actualDate is required when useLatestActualTo=false")
			}
			sql.WriteString("AND ? >= m.actual_from AND ? < m.actual_to ")
			sql.WriteString("AND ? >= m.saved_at AND ? < m.closed_at ")
			params = append(params, *actualDate, *actualDate)
			params = append(params, historyInstant, historyInstant)
		}
	case domain.ObjectRootTypeRevisionedEntity:
		sql.WriteString("AND ? >= m.saved_at AND ? < m.closed_at ")
		params = append(params, historyInstant, historyInstant)
	}

	if !predicate.IsEmpty() {
		sql.WriteString("AND ")
		sql.WriteString(predicate.SQL)
		sql.WriteString(" ")
		params = append(params, predicate.Params...)
	}

	return &subquery{sql: strings.TrimSpace(sql.String()), params: params}, nil
}

func buildSortExpressions(sortFields []sortField, fieldRegistry *FilterFieldRegistry, usage *FieldUsage) ([]string, error) {
	if len(sortFields) == 0 {
		return nil, nil
	}

	expressions := make([]string, 0, len(sortFields))
	for _, sortField := range sortFields {
		resolution, err := fieldRegistry.Resolve(sortField.selector)
		if err != nil {
			return nil, err
		}
		usage.Apply(resolution)
		expressions = append(expressions, resolution.Field.SQL)
	}
	return expressions, nil
}

func applySortLimitOffset(
	sql string,
	sortFields []sortField,
	limit *int,
	offset *int,
	params *[]any,
) string {
	result := sql
	if len(sortFields) > 0 {
		result += " ORDER BY " + buildOrderBy(sortFields)
	}
	if limit != nil {
		result += " LIMIT ?"
		*params = append(*params, *limit)
	}
	if offset != nil {
		result += " OFFSET ?"
		*params = append(*params, *offset)
	}
	return result
}

func buildOrderBy(sortFields []sortField) string {
	parts := make([]string, 0, len(sortFields))
	for idx, field := range sortFields {
		direction := "ASC"
		if !field.ascending {
			direction = "DESC"
		}
		parts = append(parts, fmt.Sprintf("sort_%d %s", idx, direction))
	}
	return strings.Join(parts, ", ")
}

func parseSort(sort []*pb.SortField) ([]sortField, error) {
	if len(sort) == 0 {
		return nil, nil
	}

	fields := make([]sortField, 0, len(sort))
	for _, token := range sort {
		if token == nil {
			return nil, fmt.Errorf("invalid sort token: nil value")
		}
		selector := strings.TrimSpace(token.Field)
		if selector == "" {
			return nil, fmt.Errorf("invalid sort selector: empty")
		}

		var ascending bool
		switch token.Direction {
		case pb.SortDirection_SORT_DIRECTION_ASC:
			ascending = true
		case pb.SortDirection_SORT_DIRECTION_DESC:
			ascending = false
		default:
			return nil, fmt.Errorf("invalid sort direction for field [%s]", selector)
		}

		fields = append(fields, sortField{
			selector:  selector,
			ascending: ascending,
		})
	}

	return fields, nil
}
