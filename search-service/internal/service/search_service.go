package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/quantara/search-service/internal/cache"
	"github.com/quantara/search-service/internal/domain"
	"github.com/quantara/search-service/internal/errdefs"
	"github.com/quantara/search-service/internal/filter"
	"github.com/quantara/search-service/internal/registry"
	"github.com/quantara/search-service/internal/repository"
	pb "github.com/quantara/search-service/proto"
)

type SearchService struct {
	repository    *repository.SearchRepository
	cache         *cache.SearchCacheService
	filterService *filter.TypedFilterService

	objectClassRegistry *registry.ObjectClassRegistry
	hierarchyRegistry   *registry.ObjectClassHierarchyRegistry
}

func NewSearchService(
	repository *repository.SearchRepository,
	cache *cache.SearchCacheService,
	filterService *filter.TypedFilterService,
	objectClassRegistry *registry.ObjectClassRegistry,
	hierarchyRegistry *registry.ObjectClassHierarchyRegistry,
) *SearchService {
	return &SearchService{
		repository:          repository,
		cache:               cache,
		filterService:       filterService,
		objectClassRegistry: objectClassRegistry,
		hierarchyRegistry:   hierarchyRegistry,
	}
}

func (s *SearchService) GetObjectRevisionByID(ctx context.Context, id int64, objectClassValue string) (string, error) {
	objectClass := s.objectClassRegistry.FromSourceValue(objectClassValue)
	if objectClass == nil {
		return "", errdefs.InvalidArgumentf("Invalid objectClass: [%s]", objectClassValue)
	}
	if objectClass.IsAbstract || strings.TrimSpace(objectClass.DataTable) == "" {
		return "", errdefs.InvalidArgumentf("objectClass is not supported for revision search: [%s]", objectClassValue)
	}

	content, err := s.repository.FindContentByID(ctx, objectClass, id)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", errdefs.NotFoundf("Object revision not found: objectClass=[%s], id=[%d]", objectClass.SourceValue, id)
		}
		return "", err
	}

	if !json.Valid([]byte(content)) {
		return "", fmt.Errorf("failed to parse JSON content")
	}
	return content, nil
}

func (s *SearchService) GetObjectByGlobalID(
	ctx context.Context,
	globalID int64,
	objectRootClassValue string,
	draftStatusValue string,
	actualDateValue string,
) (string, error) {
	objectRootClass := s.objectClassRegistry.FromSourceValue(objectRootClassValue)
	if objectRootClass == nil {
		return "", errdefs.InvalidArgumentf("Invalid objectRootClass: [%s]", objectRootClassValue)
	}

	switch objectRootClass.RootType {
	case domain.ObjectRootTypeDraftableDateBoundedEntity:
		return s.fetchGlobalDraftableByID(ctx, globalID, objectRootClass, objectRootClassValue, draftStatusValue, actualDateValue)
	case domain.ObjectRootTypeRevisionedEntity:
		return s.fetchGlobalRevisionedByID(ctx, globalID, objectRootClass, objectRootClassValue)
	default:
		return "", errdefs.InvalidArgumentf("objectRootClass is not supported for globalId search: [%s]", objectRootClassValue)
	}
}

func (s *SearchService) GetObjectCollectionByParentID(
	ctx context.Context,
	parentID int64,
	objectRootClassValue string,
) ([]string, error) {
	objectRootClass := s.objectClassRegistry.FromSourceValue(objectRootClassValue)
	if objectRootClass == nil {
		return nil, errdefs.InvalidArgumentf("Invalid objectRootClass: [%s]", objectRootClassValue)
	}
	if objectRootClass.RootType != domain.ObjectRootTypeEmbeddedEntity {
		return nil, errdefs.InvalidArgumentf("objectRootClass does not support parentId search: [%s]", objectRootClassValue)
	}

	if cached, ok := s.cache.GetParent(ctx, objectRootClass, parentID); ok {
		return cached, nil
	}

	records, err := s.repository.FindCollectionRecordsByParentID(ctx, objectRootClass, parentID)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}

	actualClass, err := s.resolveSingleActualClass(records, parentID)
	if err != nil {
		return nil, err
	}
	if err := s.validateCompatibility(objectRootClass, objectRootClassValue, actualClass); err != nil {
		return nil, err
	}

	ids := make([]int64, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.ID)
	}

	contentByID, err := s.repository.FindContentsByIDs(ctx, actualClass, ids)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(records))
	for _, record := range records {
		content, ok := contentByID[record.ID]
		if !ok {
			return nil, errdefs.NotFoundf(
				"Object revision not found: objectClass=[%s], id=[%d], parentId=[%d]",
				actualClass.SourceValue, record.ID, parentID,
			)
		}
		if !json.Valid([]byte(content)) {
			return nil, fmt.Errorf("failed to parse JSON content")
		}
		result = append(result, content)
	}

	s.cache.PutParent(ctx, actualClass, parentID, result)
	return result, nil
}

func (s *SearchService) GetObjectGlobalIDByAltID(
	ctx context.Context,
	altID string,
	sourceAlias string,
	objectRootClassValue string,
) (int64, error) {
	if strings.TrimSpace(altID) == "" {
		return 0, errdefs.InvalidArgumentf("altId is required")
	}
	if strings.TrimSpace(sourceAlias) == "" {
		return 0, errdefs.InvalidArgumentf("sourceAlias is required")
	}

	objectRootClass := s.objectClassRegistry.FromSourceValue(objectRootClassValue)
	if objectRootClass == nil {
		return 0, errdefs.InvalidArgumentf("Invalid objectRootClass: [%s]", objectRootClassValue)
	}

	if cached, ok := s.cache.GetGlobalIDByAltID(ctx, objectRootClass, sourceAlias, altID); ok {
		return cached, nil
	}

	altRootClass := s.objectClassRegistry.ByName("ALTERNATIVE_IDENTIFIER")
	if altRootClass == nil {
		return 0, fmt.Errorf("ALTERNATIVE_IDENTIFIER class is not configured")
	}
	sourceClass := s.objectClassRegistry.ByName("SOURCE")
	if sourceClass == nil {
		return 0, fmt.Errorf("SOURCE class is not configured")
	}

	globalID, err := s.repository.FindGlobalIDByAltIDAndSourceAlias(
		ctx,
		altID,
		sourceAlias,
		altRootClass,
		sourceClass,
		objectRootClass,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, errdefs.NotFoundf(
				"GlobalId not found for altId=[%s], sourceAlias=[%s], objectRootClass=[%s]",
				altID,
				sourceAlias,
				objectRootClassValue,
			)
		}
		return 0, err
	}

	s.cache.PutGlobalIDByAltID(ctx, objectRootClass, sourceAlias, altID, globalID)
	return globalID, nil
}

func (s *SearchService) GetObjectCollectionByFilter(
	ctx context.Context,
	objectRootClassValue string,
	draftStatusValue string,
	actualDateValue string,
	filterExpr *pb.FilterExpr,
	sort []*pb.SortField,
	limit *int,
	offset *int,
) ([]string, error) {
	objectRootClass := s.objectClassRegistry.FromSourceValue(objectRootClassValue)
	if objectRootClass == nil {
		return nil, errdefs.InvalidArgumentf("Invalid objectRootClass: [%s]", objectRootClassValue)
	}

	rootType := objectRootClass.RootType
	if rootType == domain.ObjectRootTypeNone {
		return nil, errdefs.InvalidArgumentf("objectRootClass does not support filtering: [%s]", objectRootClassValue)
	}

	actualClasses := s.hierarchyRegistry.ActualClassesForRoot(objectRootClass)
	if len(actualClasses) == 0 {
		return nil, errdefs.InvalidArgumentf("objectRootClass does not support filtering: [%s]", objectRootClassValue)
	}

	draftStatus := domain.DraftStatusConfirmed
	var actualDate *time.Time
	useLatestActualTo := false
	if rootType == domain.ObjectRootTypeDraftableDateBoundedEntity {
		parsedStatus, err := parseDraftStatus(draftStatusValue)
		if err != nil {
			return nil, err
		}
		draftStatus = parsedStatus

		parsedDate, err := parseActualDateOrNil(actualDateValue)
		if err != nil {
			return nil, err
		}
		actualDate = parsedDate
		if actualDate == nil {
			useLatestActualTo = true
		}
	}

	historyInstant := time.Now().UTC()
	query, err := s.filterService.BuildQuery(
		objectRootClass,
		actualClasses,
		draftStatus == domain.DraftStatusDraft,
		actualDate,
		historyInstant,
		filterExpr,
		sort,
		limit,
		offset,
		useLatestActualTo,
	)
	if err != nil {
		return nil, err
	}

	contents, err := s.repository.FindContentsByQuery(ctx, query.SQL, query.Params)
	if err != nil {
		return nil, err
	}

	for _, content := range contents {
		if !json.Valid([]byte(content)) {
			return nil, fmt.Errorf("failed to parse JSON content")
		}
	}
	return contents, nil
}

func (s *SearchService) fetchGlobalRevisionedByID(
	ctx context.Context,
	globalID int64,
	objectRootClass *domain.ObjectClassInfo,
	objectRootClassValue string,
) (string, error) {
	if cached, ok := s.cache.GetGlobalContract(ctx, objectRootClass, globalID); ok {
		return cached, nil
	}

	historyInstant := time.Now().UTC()
	mainRecord, err := s.repository.FindRevisionedMainRecord(ctx, objectRootClass, globalID, historyInstant)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", errdefs.NotFoundf("Object not found for globalId=[%d], objectRootClass=[%s]", globalID, objectRootClassValue)
		}
		return "", err
	}

	actualClass := s.objectClassRegistry.FromSourceValue(mainRecord.ObjectClass)
	if actualClass == nil {
		return "", fmt.Errorf("unknown objectClass in main table: [%s]", mainRecord.ObjectClass)
	}
	if err := s.validateCompatibility(objectRootClass, objectRootClassValue, actualClass); err != nil {
		return "", err
	}

	content, err := s.repository.FindContentByID(ctx, actualClass, mainRecord.ID)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", errdefs.NotFoundf("Object revision not found: objectClass=[%s], id=[%d]", actualClass.SourceValue, mainRecord.ID)
		}
		return "", err
	}
	if !json.Valid([]byte(content)) {
		return "", fmt.Errorf("failed to parse JSON content")
	}

	s.cache.PutGlobalContract(ctx, globalID, mainRecord.ObjectClass, content)
	return content, nil
}

func (s *SearchService) fetchGlobalDraftableByID(
	ctx context.Context,
	globalID int64,
	objectRootClass *domain.ObjectClassInfo,
	objectRootClassValue string,
	draftStatusValue string,
	actualDateValue string,
) (string, error) {
	draftStatus, err := parseDraftStatus(draftStatusValue)
	if err != nil {
		return "", err
	}
	actualDate, err := parseActualDateOrNil(actualDateValue)
	if err != nil {
		return "", err
	}
	actualDateProvided := actualDate != nil

	if !actualDateProvided {
		if cached, ok := s.cache.GetGlobalTrade(ctx, objectRootClass, globalID, draftStatus); ok {
			return cached, nil
		}
	}

	historyInstant := time.Now().UTC()
	isDraft := draftStatus == domain.DraftStatusDraft

	var mainRecord *domain.MainRecord
	if actualDateProvided {
		mainRecord, err = s.repository.FindDraftableDateBoundedMainRecord(
			ctx,
			objectRootClass,
			globalID,
			isDraft,
			*actualDate,
			historyInstant,
		)
	} else {
		mainRecord, err = s.repository.FindLatestDraftableDateBoundedMainRecord(
			ctx,
			objectRootClass,
			globalID,
			isDraft,
			historyInstant,
		)
	}
	if err != nil {
		if err == sql.ErrNoRows {
			return "", errdefs.NotFoundf("Object not found for globalId=[%d], objectRootClass=[%s]", globalID, objectRootClassValue)
		}
		return "", err
	}

	actualClass := s.objectClassRegistry.FromSourceValue(mainRecord.ObjectClass)
	if actualClass == nil {
		return "", fmt.Errorf("unknown objectClass in main table: [%s]", mainRecord.ObjectClass)
	}
	if err := s.validateCompatibility(objectRootClass, objectRootClassValue, actualClass); err != nil {
		return "", err
	}

	content, err := s.repository.FindContentByID(ctx, actualClass, mainRecord.ID)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", errdefs.NotFoundf("Object revision not found: objectClass=[%s], id=[%d]", actualClass.SourceValue, mainRecord.ID)
		}
		return "", err
	}
	if !json.Valid([]byte(content)) {
		return "", fmt.Errorf("failed to parse JSON content")
	}

	if !actualDateProvided {
		s.cache.PutGlobalTrade(ctx, globalID, draftStatus, mainRecord.ObjectClass, content)
	}
	return content, nil
}

func parseDraftStatus(value string) (domain.DraftStatus, error) {
	status, err := domain.ParseDraftStatus(value)
	if err != nil {
		return "", errdefs.InvalidArgumentf("Invalid draftStatus")
	}
	if status == "" {
		return domain.DraftStatusConfirmed, nil
	}
	return status, nil
}

func parseActualDateOrNil(value string) (*time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, nil
	}
	parsed, err := time.Parse("2006-01-02", trimmed)
	if err != nil {
		return nil, errdefs.InvalidArgumentf("Invalid actualDate format: [%s], expected yyyy-MM-dd", value)
	}
	return &parsed, nil
}

func (s *SearchService) validateCompatibility(
	objectRootClass *domain.ObjectClassInfo,
	objectRootClassValue string,
	actualClass *domain.ObjectClassInfo,
) error {
	if !s.hierarchyRegistry.IsParentOrSelf(objectRootClass, actualClass) {
		return errdefs.NotFoundf(
			"Object class mismatch: requested=[%s], actual=[%s]",
			objectRootClassValue,
			actualClass.SourceValue,
		)
	}
	return nil
}

func (s *SearchService) resolveSingleActualClass(records []domain.MainRecord, parentID int64) (*domain.ObjectClassInfo, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records for parentId=[%d]", parentID)
	}

	var actualClass *domain.ObjectClassInfo
	for _, record := range records {
		candidate := s.objectClassRegistry.FromSourceValue(record.ObjectClass)
		if candidate == nil {
			return nil, fmt.Errorf("unknown objectClass in main table: [%s]", record.ObjectClass)
		}
		if actualClass == nil {
			actualClass = candidate
			continue
		}
		if candidate != actualClass {
			return nil, fmt.Errorf(
				"multiple objectClass values for parentId=[%d]: [%s], [%s]",
				parentID,
				actualClass.SourceValue,
				candidate.SourceValue,
			)
		}
	}
	return actualClass, nil
}
