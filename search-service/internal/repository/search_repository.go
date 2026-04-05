package repository

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/quantara/search-service/internal/dbschema"
	"github.com/quantara/search-service/internal/domain"
	"github.com/quantara/search-service/internal/errdefs"
)

var (
	columnNamePattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	maxActualTo       = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)
)

type SearchRepository struct {
	db            *sql.DB
	defaultSchema string
}

func NewSearchRepository(db *sql.DB, defaultSchema string) *SearchRepository {
	return &SearchRepository{
		db:            db,
		defaultSchema: strings.TrimSpace(defaultSchema),
	}
}

func (r *SearchRepository) FindContentByID(ctx context.Context, objectClass *domain.ObjectClassInfo, id int64) (string, error) {
	table, err := r.validateTableName(objectClass.DataTable)
	if err != nil {
		return "", err
	}

	query := toPostgresPlaceholders("SELECT content FROM " + table + " WHERE id = ?")
	var content string
	if err := r.db.QueryRowContext(ctx, query, id).Scan(&content); err != nil {
		return "", err
	}
	return content, nil
}

func (r *SearchRepository) FindDraftableDateBoundedMainRecord(
	ctx context.Context,
	rootClass *domain.ObjectClassInfo,
	globalID int64,
	draftStatus bool,
	actualDate time.Time,
	historyInstant time.Time,
) (*domain.MainRecord, error) {
	table, err := r.validateTableName(rootClass.MainTable)
	if err != nil {
		return nil, err
	}

	query := toPostgresPlaceholders(
		"SELECT id, object_class FROM " + table + " " +
			"WHERE global_id = ? " +
			"AND draft_status = ? " +
			"AND ? >= actual_from " +
			"AND ? < actual_to " +
			"AND ? >= saved_at " +
			"AND ? < closed_at " +
			"FOR SHARE",
	)

	record := &domain.MainRecord{}
	err = r.db.QueryRowContext(
		ctx,
		query,
		globalID,
		draftStatus,
		actualDate,
		actualDate,
		historyInstant,
		historyInstant,
	).Scan(&record.ID, &record.ObjectClass)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (r *SearchRepository) FindLatestDraftableDateBoundedMainRecord(
	ctx context.Context,
	rootClass *domain.ObjectClassInfo,
	globalID int64,
	draftStatus bool,
	historyInstant time.Time,
) (*domain.MainRecord, error) {
	table, err := r.validateTableName(rootClass.MainTable)
	if err != nil {
		return nil, err
	}

	query := toPostgresPlaceholders(
		"SELECT id, object_class FROM " + table + " " +
			"WHERE global_id = ? " +
			"AND draft_status = ? " +
			"AND actual_to = ? " +
			"AND ? >= saved_at " +
			"AND ? < closed_at " +
			"FOR SHARE",
	)

	record := &domain.MainRecord{}
	err = r.db.QueryRowContext(
		ctx,
		query,
		globalID,
		draftStatus,
		maxActualTo,
		historyInstant,
		historyInstant,
	).Scan(&record.ID, &record.ObjectClass)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (r *SearchRepository) FindRevisionedMainRecord(
	ctx context.Context,
	rootClass *domain.ObjectClassInfo,
	globalID int64,
	historyInstant time.Time,
) (*domain.MainRecord, error) {
	table, err := r.validateTableName(rootClass.MainTable)
	if err != nil {
		return nil, err
	}

	query := toPostgresPlaceholders(
		"SELECT id, object_class FROM " + table + " " +
			"WHERE global_id = ? " +
			"AND ? >= saved_at " +
			"AND ? < closed_at " +
			"FOR SHARE",
	)

	record := &domain.MainRecord{}
	err = r.db.QueryRowContext(
		ctx,
		query,
		globalID,
		historyInstant,
		historyInstant,
	).Scan(&record.ID, &record.ObjectClass)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (r *SearchRepository) FindCollectionRecordsByParentID(
	ctx context.Context,
	objectRootClass *domain.ObjectClassInfo,
	parentID int64,
) ([]domain.MainRecord, error) {
	table, err := r.validateTableName(objectRootClass.MainTable)
	if err != nil {
		return nil, err
	}

	query := toPostgresPlaceholders("SELECT id, object_class FROM " + table + " WHERE parent_id = ? ORDER BY id")
	rows, err := r.db.QueryContext(ctx, query, parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMainRecords(rows)
}

func (r *SearchRepository) FindContentsByIDs(
	ctx context.Context,
	objectClass *domain.ObjectClassInfo,
	ids []int64,
) (map[int64]string, error) {
	if len(ids) == 0 {
		return map[int64]string{}, nil
	}

	table, err := r.validateTableName(objectClass.DataTable)
	if err != nil {
		return nil, err
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	query := toPostgresPlaceholders("SELECT id, content FROM " + table + " WHERE id IN (" + placeholders + ")")

	params := make([]any, 0, len(ids))
	for _, id := range ids {
		params = append(params, id)
	}

	rows, err := r.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int64]string, len(ids))
	for rows.Next() {
		var id int64
		var content string
		if err := rows.Scan(&id, &content); err != nil {
			return nil, err
		}
		result[id] = content
	}
	return result, rows.Err()
}

func (r *SearchRepository) FindGlobalIDByAltIDAndSourceAlias(
	ctx context.Context,
	altID string,
	sourceAlias string,
	altRootClass *domain.ObjectClassInfo,
	sourceClass *domain.ObjectClassInfo,
	parentClass *domain.ObjectClassInfo,
) (int64, error) {
	altMainTable, err := r.validateTableName(altRootClass.MainTable)
	if err != nil {
		return 0, err
	}
	altIndexTable, err := r.validateTableName(altRootClass.IndexTable)
	if err != nil {
		return 0, err
	}
	sourceMainTable, err := r.validateTableName(sourceClass.MainTable)
	if err != nil {
		return 0, err
	}
	sourceIndexTable, err := r.validateTableName(sourceClass.IndexTable)
	if err != nil {
		return 0, err
	}
	parentMainTable, err := r.validateTableName(parentClass.MainTable)
	if err != nil {
		return 0, err
	}

	altValueColumn, err := resolveIndexColumn(altRootClass, "value")
	if err != nil {
		return 0, err
	}
	altSourceIDColumn, err := resolveIndexColumn(altRootClass, "sourceId")
	if err != nil {
		return 0, err
	}
	altParentIDColumn, err := resolveMainColumn(altRootClass, "parentId")
	if err != nil {
		return 0, err
	}
	altMainIDColumn, err := resolveMainColumn(altRootClass, "id")
	if err != nil {
		return 0, err
	}
	sourceAliasColumn, err := resolveIndexColumn(sourceClass, "alias")
	if err != nil {
		return 0, err
	}
	sourceIDColumn, err := resolveMainColumn(sourceClass, "id")
	if err != nil {
		return 0, err
	}
	parentIDColumn, err := resolveMainColumn(parentClass, "id")
	if err != nil {
		return 0, err
	}
	parentGlobalIDColumn, err := resolveMainColumn(parentClass, "globalId")
	if err != nil {
		return 0, err
	}

	query := toPostgresPlaceholders(
		"SELECT pm." + parentGlobalIDColumn + " " +
			"FROM " + altIndexTable + " aii " +
			"JOIN " + altMainTable + " aim ON aim." + altMainIDColumn + " = aii." + altMainIDColumn + " " +
			"JOIN " + sourceMainTable + " sm ON sm." + sourceIDColumn + " = aii." + altSourceIDColumn + " " +
			"JOIN " + sourceIndexTable + " si ON si.id = sm." + sourceIDColumn + " " +
			"JOIN " + parentMainTable + " pm ON pm." + parentIDColumn + " = aim." + altParentIDColumn + " " +
			"WHERE aii." + altValueColumn + " = ? " +
			"AND si." + sourceAliasColumn + " = ? " +
			"FOR SHARE",
	)

	var globalID int64
	if err := r.db.QueryRowContext(ctx, query, altID, sourceAlias).Scan(&globalID); err != nil {
		return 0, err
	}
	return globalID, nil
}

func (r *SearchRepository) FindContentsByQuery(
	ctx context.Context,
	query string,
	params []any,
) ([]string, error) {
	if strings.TrimSpace(query) == "" {
		return nil, nil
	}

	rows, err := r.db.QueryContext(ctx, toPostgresPlaceholders(query), params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]string, 0)
	for rows.Next() {
		var content string
		if err := rows.Scan(&content); err != nil {
			return nil, err
		}
		result = append(result, content)
	}
	return result, rows.Err()
}

func scanMainRecords(rows *sql.Rows) ([]domain.MainRecord, error) {
	result := make([]domain.MainRecord, 0)
	for rows.Next() {
		var record domain.MainRecord
		if err := rows.Scan(&record.ID, &record.ObjectClass); err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func resolveIndexColumn(objectClass *domain.ObjectClassInfo, field string) (string, error) {
	if objectClass == nil {
		return "", errdefs.InvalidArgumentf("object class is not configured")
	}
	column, ok := objectClass.IndexColumns[normalize(field)]
	if !ok {
		return "", errdefs.InvalidArgumentf("index field not defined for class: [%s]", field)
	}
	return validateColumnName(column.DBColumn)
}

func resolveMainColumn(objectClass *domain.ObjectClassInfo, field string) (string, error) {
	if objectClass == nil {
		return "", errdefs.InvalidArgumentf("object class is not configured")
	}
	column, ok := objectClass.MainColumns[normalize(field)]
	if !ok {
		return "", errdefs.InvalidArgumentf("main field not defined for class: [%s]", field)
	}
	return validateColumnName(column.DBColumn)
}

func (r *SearchRepository) validateTableName(name string) (string, error) {
	qualified, err := dbschema.QualifyTable(name, r.defaultSchema)
	if err != nil {
		return "", errdefs.InvalidArgumentf("%s", err)
	}
	return qualified, nil
}

func validateColumnName(name string) (string, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", errdefs.InvalidArgumentf("column is not configured")
	}
	if !columnNamePattern.MatchString(trimmed) {
		return "", errdefs.InvalidArgumentf("invalid column name: [%s]", name)
	}
	return trimmed, nil
}

func toPostgresPlaceholders(query string) string {
	var builder strings.Builder
	builder.Grow(len(query) + 16)

	placeholder := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			builder.WriteString(fmt.Sprintf("$%d", placeholder))
			placeholder++
			continue
		}
		builder.WriteByte(query[i])
	}
	return builder.String()
}

func normalize(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
