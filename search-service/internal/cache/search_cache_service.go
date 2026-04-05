package cache

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/quantara/search-service/internal/domain"
	"github.com/quantara/search-service/internal/registry"
)

type SearchCacheService struct {
	client redis.UniversalClient

	objectClassRegistry *registry.ObjectClassRegistry
	hierarchyRegistry   *registry.ObjectClassHierarchyRegistry
}

func NewSearchCacheService(
	client redis.UniversalClient,
	objectClassRegistry *registry.ObjectClassRegistry,
	hierarchyRegistry *registry.ObjectClassHierarchyRegistry,
) *SearchCacheService {
	return &SearchCacheService{
		client:              client,
		objectClassRegistry: objectClassRegistry,
		hierarchyRegistry:   hierarchyRegistry,
	}
}

func (s *SearchCacheService) GetGlobalContract(ctx context.Context, objectRootClass *domain.ObjectClassInfo, globalID int64) (string, bool) {
	return s.getGlobalByKey(ctx, objectRootClass, globalKeyForContract(globalID))
}

func (s *SearchCacheService) GetGlobalTrade(
	ctx context.Context,
	objectRootClass *domain.ObjectClassInfo,
	globalID int64,
	draftStatus domain.DraftStatus,
) (string, bool) {
	suffix := "confirmed"
	if draftStatus == domain.DraftStatusDraft {
		suffix = "draft"
	}
	return s.getGlobalByKey(ctx, objectRootClass, globalKeyForTrade(globalID, suffix))
}

func (s *SearchCacheService) PutGlobalContract(
	ctx context.Context,
	globalID int64,
	actualClassValue string,
	content string,
) {
	s.putGlobalByKey(ctx, globalKeyForContract(globalID), actualClassValue, content)
}

func (s *SearchCacheService) PutGlobalTrade(
	ctx context.Context,
	globalID int64,
	draftStatus domain.DraftStatus,
	actualClassValue string,
	content string,
) {
	suffix := "confirmed"
	if draftStatus == domain.DraftStatusDraft {
		suffix = "draft"
	}
	s.putGlobalByKey(ctx, globalKeyForTrade(globalID, suffix), actualClassValue, content)
}

func (s *SearchCacheService) GetGlobalIDByAltID(
	ctx context.Context,
	objectRootClass *domain.ObjectClassInfo,
	sourceAlias string,
	altID string,
) (int64, bool) {
	if s.client == nil || objectRootClass == nil || strings.TrimSpace(sourceAlias) == "" || strings.TrimSpace(altID) == "" {
		return 0, false
	}

	key := buildAltIDKey(objectRootClass, sourceAlias, altID)
	value, err := s.client.Get(ctx, key).Result()
	if err != nil || strings.TrimSpace(value) == "" {
		return 0, false
	}

	globalID, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return globalID, true
}

func (s *SearchCacheService) PutGlobalIDByAltID(
	ctx context.Context,
	objectRootClass *domain.ObjectClassInfo,
	sourceAlias string,
	altID string,
	globalID int64,
) {
	if s.client == nil || objectRootClass == nil || strings.TrimSpace(sourceAlias) == "" || strings.TrimSpace(altID) == "" {
		return
	}

	key := buildAltIDKey(objectRootClass, sourceAlias, altID)
	_ = s.client.Set(ctx, key, strconv.FormatInt(globalID, 10), 0).Err()
}

func (s *SearchCacheService) GetParent(
	ctx context.Context,
	objectRootClass *domain.ObjectClassInfo,
	parentID int64,
) ([]string, bool) {
	if s.client == nil || objectRootClass == nil {
		return nil, false
	}

	key := buildParentKey(objectRootClass, parentID)
	values, err := s.client.LRange(ctx, key, 0, -1).Result()
	if err != nil || len(values) == 0 {
		return nil, false
	}

	for _, value := range values {
		if !json.Valid([]byte(value)) {
			return nil, false
		}
	}
	return values, true
}

func (s *SearchCacheService) PutParent(
	ctx context.Context,
	actualClass *domain.ObjectClassInfo,
	parentID int64,
	items []string,
) {
	if s.client == nil || actualClass == nil || len(items) == 0 {
		return
	}

	args := make([]any, 0, len(items))
	for _, item := range items {
		args = append(args, item)
	}

	key := buildParentKey(actualClass, parentID)
	_ = s.client.RPush(ctx, key, args...).Err()
}

func (s *SearchCacheService) getGlobalByKey(
	ctx context.Context,
	objectRootClass *domain.ObjectClassInfo,
	key string,
) (string, bool) {
	if s.client == nil || objectRootClass == nil {
		return "", false
	}

	entries, err := s.client.HGetAll(ctx, key).Result()
	if err != nil || len(entries) == 0 {
		return "", false
	}

	actualClassValue := entries["objectClass"]
	body := entries["body"]
	if strings.TrimSpace(actualClassValue) == "" || strings.TrimSpace(body) == "" {
		return "", false
	}

	actualClass := s.objectClassRegistry.FromSourceValue(actualClassValue)
	if actualClass == nil || !s.hierarchyRegistry.IsParentOrSelf(objectRootClass, actualClass) {
		return "", false
	}

	if !json.Valid([]byte(body)) {
		return "", false
	}
	return body, true
}

func (s *SearchCacheService) putGlobalByKey(
	ctx context.Context,
	key string,
	actualClassValue string,
	content string,
) {
	if s.client == nil || strings.TrimSpace(actualClassValue) == "" || strings.TrimSpace(content) == "" {
		return
	}

	values := map[string]any{
		"objectClass": actualClassValue,
		"body":        content,
	}
	_ = s.client.HSet(ctx, key, values).Err()
}

func globalKeyForContract(globalID int64) string {
	return "txn:committed:global:" + strconv.FormatInt(globalID, 10)
}

func globalKeyForTrade(globalID int64, suffix string) string {
	return "txn:committed:global:" + strconv.FormatInt(globalID, 10) + ":" + suffix
}

func buildParentKey(actualClass *domain.ObjectClassInfo, parentID int64) string {
	return "txn:committed:" + actualClass.SourceValue + ":" + strconv.FormatInt(parentID, 10)
}

func buildAltIDKey(objectRootClass *domain.ObjectClassInfo, sourceAlias, altID string) string {
	return "alt:" + objectRootClass.SourceValue + ":" + sourceAlias + ":" + altID
}
