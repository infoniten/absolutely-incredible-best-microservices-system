package filter

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/quantara/search-service/internal/domain"
	pb "github.com/quantara/search-service/proto"
)

var (
	numberPattern = regexp.MustCompile(`^[-+]?\d+(\.\d+)?$`)
	datePattern   = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
)

type FieldRef struct {
	SQL  string
	Type *domain.ColumnType
	JSON bool
}

type FieldResolver func(selector string) (FieldRef, error)

type SqlPredicate struct {
	SQL    string
	Params []any
}

func (p SqlPredicate) IsEmpty() bool {
	return strings.TrimSpace(p.SQL) == ""
}

func EmptyPredicate() SqlPredicate {
	return SqlPredicate{SQL: "", Params: nil}
}

type TypedFilterSQLBuilder struct{}

func NewTypedFilterSQLBuilder() *TypedFilterSQLBuilder {
	return &TypedFilterSQLBuilder{}
}

func (b *TypedFilterSQLBuilder) ToPredicate(filter *pb.FilterExpr, fieldResolver FieldResolver) (SqlPredicate, error) {
	if filter == nil || filter.Expr == nil {
		return EmptyPredicate(), nil
	}
	return b.buildExpr(filter, fieldResolver)
}

func (b *TypedFilterSQLBuilder) buildExpr(expr *pb.FilterExpr, fieldResolver FieldResolver) (SqlPredicate, error) {
	switch v := expr.Expr.(type) {
	case *pb.FilterExpr_And:
		children := make([]SqlPredicate, 0, len(v.And.Children))
		for _, child := range v.And.Children {
			predicate, err := b.buildExpr(child, fieldResolver)
			if err != nil {
				return SqlPredicate{}, err
			}
			if !predicate.IsEmpty() {
				children = append(children, predicate)
			}
		}
		return combinePredicates(children, "AND"), nil
	case *pb.FilterExpr_Or:
		children := make([]SqlPredicate, 0, len(v.Or.Children))
		for _, child := range v.Or.Children {
			predicate, err := b.buildExpr(child, fieldResolver)
			if err != nil {
				return SqlPredicate{}, err
			}
			if !predicate.IsEmpty() {
				children = append(children, predicate)
			}
		}
		return combinePredicates(children, "OR"), nil
	case *pb.FilterExpr_Not:
		if v.Not == nil || v.Not.Child == nil {
			return SqlPredicate{}, fmt.Errorf("NOT expression requires child")
		}
		child, err := b.buildExpr(v.Not.Child, fieldResolver)
		if err != nil {
			return SqlPredicate{}, err
		}
		if child.IsEmpty() {
			return EmptyPredicate(), nil
		}
		return SqlPredicate{
			SQL:    "NOT (" + child.SQL + ")",
			Params: child.Params,
		}, nil
	case *pb.FilterExpr_Predicate:
		return b.buildPredicate(v.Predicate, fieldResolver)
	default:
		return SqlPredicate{}, fmt.Errorf("unknown filter expression")
	}
}

func (b *TypedFilterSQLBuilder) buildPredicate(predicate *pb.FilterPredicate, fieldResolver FieldResolver) (SqlPredicate, error) {
	if predicate == nil {
		return SqlPredicate{}, fmt.Errorf("predicate is nil")
	}
	selector := strings.TrimSpace(predicate.Field)
	if selector == "" {
		return SqlPredicate{}, fmt.Errorf("predicate field is empty")
	}

	field, err := fieldResolver(selector)
	if err != nil {
		return SqlPredicate{}, err
	}

	operator := predicate.Operator
	switch operator {
	case pb.FilterOperator_FILTER_OPERATOR_IS_NULL:
		if err := requireNoValues(predicate.Values, operator); err != nil {
			return SqlPredicate{}, err
		}
		return SqlPredicate{SQL: field.SQL + " IS NULL"}, nil
	case pb.FilterOperator_FILTER_OPERATOR_IS_NOT_NULL:
		if err := requireNoValues(predicate.Values, operator); err != nil {
			return SqlPredicate{}, err
		}
		return SqlPredicate{SQL: field.SQL + " IS NOT NULL"}, nil
	}

	values, err := filterValuesToStrings(predicate.Values)
	if err != nil {
		return SqlPredicate{}, err
	}

	switch operator {
	case pb.FilterOperator_FILTER_OPERATOR_LIKE:
		if len(values) != 1 {
			return SqlPredicate{}, fmt.Errorf("operator [%s] expects [1] argument(s)", operator.String())
		}
		if field.Type != nil && *field.Type != domain.ColumnTypeString {
			return SqlPredicate{}, fmt.Errorf("LIKE is only supported for string fields")
		}
		return SqlPredicate{
			SQL:    field.SQL + " ILIKE ?",
			Params: []any{strings.ReplaceAll(values[0], "*", "%")},
		}, nil
	case pb.FilterOperator_FILTER_OPERATOR_IN, pb.FilterOperator_FILTER_OPERATOR_NOT_IN:
		if len(values) == 0 {
			return SqlPredicate{}, fmt.Errorf("IN/NOT_IN requires at least one argument")
		}
		resolvedSQL, params, err := resolveField(field, operator, values)
		if err != nil {
			return SqlPredicate{}, err
		}
		placeholder := strings.TrimSuffix(strings.Repeat("?, ", len(values)), ", ")
		sql := resolvedSQL + " IN (" + placeholder + ")"
		if operator == pb.FilterOperator_FILTER_OPERATOR_NOT_IN {
			sql = resolvedSQL + " NOT IN (" + placeholder + ")"
		}
		return SqlPredicate{SQL: sql, Params: params}, nil
	case pb.FilterOperator_FILTER_OPERATOR_EQ,
		pb.FilterOperator_FILTER_OPERATOR_NE,
		pb.FilterOperator_FILTER_OPERATOR_GT,
		pb.FilterOperator_FILTER_OPERATOR_GE,
		pb.FilterOperator_FILTER_OPERATOR_LT,
		pb.FilterOperator_FILTER_OPERATOR_LE:
		if len(values) != 1 {
			return SqlPredicate{}, fmt.Errorf("operator [%s] expects [1] argument(s)", operator.String())
		}

		resolvedSQL, params, err := resolveField(field, operator, values)
		if err != nil {
			return SqlPredicate{}, err
		}

		var sql string
		switch operator {
		case pb.FilterOperator_FILTER_OPERATOR_EQ:
			sql = resolvedSQL + " = ?"
		case pb.FilterOperator_FILTER_OPERATOR_NE:
			sql = resolvedSQL + " <> ?"
		case pb.FilterOperator_FILTER_OPERATOR_GT:
			sql = resolvedSQL + " > ?"
		case pb.FilterOperator_FILTER_OPERATOR_GE:
			sql = resolvedSQL + " >= ?"
		case pb.FilterOperator_FILTER_OPERATOR_LT:
			sql = resolvedSQL + " < ?"
		case pb.FilterOperator_FILTER_OPERATOR_LE:
			sql = resolvedSQL + " <= ?"
		}

		return SqlPredicate{SQL: sql, Params: params}, nil
	default:
		return SqlPredicate{}, fmt.Errorf("unsupported operator: [%s]", operator.String())
	}
}

func combinePredicates(parts []SqlPredicate, operator string) SqlPredicate {
	if len(parts) == 0 {
		return EmptyPredicate()
	}
	if len(parts) == 1 {
		return parts[0]
	}

	var sql strings.Builder
	params := make([]any, 0)
	for i, part := range parts {
		if i > 0 {
			sql.WriteString(" ")
			sql.WriteString(operator)
			sql.WriteString(" ")
		}
		sql.WriteString("(")
		sql.WriteString(part.SQL)
		sql.WriteString(")")
		params = append(params, part.Params...)
	}
	return SqlPredicate{SQL: sql.String(), Params: params}
}

func filterValuesToStrings(values []*pb.FilterValue) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}

	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == nil || value.Kind == nil {
			return nil, fmt.Errorf("filter value is nil")
		}
		switch v := value.Kind.(type) {
		case *pb.FilterValue_StringValue:
			result = append(result, v.StringValue)
		case *pb.FilterValue_Int64Value:
			result = append(result, strconv.FormatInt(v.Int64Value, 10))
		case *pb.FilterValue_Int32Value:
			result = append(result, strconv.FormatInt(int64(v.Int32Value), 10))
		case *pb.FilterValue_DoubleValue:
			result = append(result, strconv.FormatFloat(v.DoubleValue, 'f', -1, 64))
		case *pb.FilterValue_BoolValue:
			if v.BoolValue {
				result = append(result, "true")
			} else {
				result = append(result, "false")
			}
		case *pb.FilterValue_DateValue:
			result = append(result, v.DateValue)
		case *pb.FilterValue_DatetimeValue:
			result = append(result, v.DatetimeValue)
		default:
			return nil, fmt.Errorf("unknown filter value kind")
		}
	}
	return result, nil
}

func requireNoValues(values []*pb.FilterValue, op pb.FilterOperator) error {
	if len(values) > 0 {
		return fmt.Errorf("operator [%s] does not accept values", op.String())
	}
	return nil
}

func resolveField(field FieldRef, operator pb.FilterOperator, args []string) (string, []any, error) {
	var effectiveType *domain.ColumnType
	if field.Type != nil {
		copied := *field.Type
		effectiveType = &copied
	}

	if field.JSON && effectiveType == nil && operator != pb.FilterOperator_FILTER_OPERATOR_LIKE {
		if allNumbers(args) {
			t := domain.ColumnTypeDecimal
			effectiveType = &t
		} else if allDates(args) {
			t := domain.ColumnTypeDate
			effectiveType = &t
		}
	}

	sql := field.SQL
	if field.JSON && effectiveType != nil {
		switch *effectiveType {
		case domain.ColumnTypeDecimal:
			sql = "(" + sql + ")::numeric"
		case domain.ColumnTypeDate:
			sql = "(" + sql + ")::date"
		case domain.ColumnTypeDateTime:
			sql = "(" + sql + ")::timestamptz"
		case domain.ColumnTypeBoolean:
			sql = "(" + sql + ")::boolean"
		case domain.ColumnTypeLong:
			sql = "(" + sql + ")::bigint"
		case domain.ColumnTypeInteger:
			sql = "(" + sql + ")::integer"
		}
	}

	parsedArgs, err := parseArgs(effectiveType, args)
	if err != nil {
		return "", nil, err
	}

	return sql, parsedArgs, nil
}

func parseArgs(columnType *domain.ColumnType, args []string) ([]any, error) {
	if len(args) == 0 {
		return nil, nil
	}

	if columnType == nil || *columnType == domain.ColumnTypeString {
		result := make([]any, 0, len(args))
		for _, arg := range args {
			result = append(result, arg)
		}
		return result, nil
	}

	result := make([]any, 0, len(args))
	for _, arg := range args {
		parsed, err := columnType.Parse(arg)
		if err != nil {
			return nil, err
		}
		result = append(result, parsed)
	}
	return result, nil
}

func allNumbers(values []string) bool {
	for _, value := range values {
		if !numberPattern.MatchString(value) {
			return false
		}
	}
	return true
}

func allDates(values []string) bool {
	for _, value := range values {
		if !datePattern.MatchString(value) {
			return false
		}
	}
	return true
}
