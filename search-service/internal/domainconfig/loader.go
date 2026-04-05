package domainconfig

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/quantara/search-service/internal/domain"
)

type Loader struct {
	remoteURL string
	localFile string

	mu   sync.RWMutex
	info *domain.DomainConfigInfo
}

func New(remoteURL, localFile string) *Loader {
	return &Loader{
		remoteURL: strings.TrimSpace(remoteURL),
		localFile: strings.TrimSpace(localFile),
	}
}

func (l *Loader) Load(ctx context.Context) (*domain.SearchDomainConfig, error) {
	if cfg, source, location, err := l.loadRemote(ctx); err == nil && cfg != nil {
		l.setInfo(buildInfo(source, location, cfg))
		return cfg, nil
	}

	cfg, source, location, err := l.loadLocal()
	if err != nil {
		return nil, err
	}
	l.setInfo(buildInfo(source, location, cfg))
	return cfg, nil
}

func (l *Loader) Info() (*domain.DomainConfigInfo, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.info == nil {
		return nil, fmt.Errorf("domain config is not loaded")
	}
	return l.info, nil
}

func (l *Loader) setInfo(info *domain.DomainConfigInfo) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.info = info
}

func (l *Loader) loadRemote(ctx context.Context) (*domain.SearchDomainConfig, string, string, error) {
	if l.remoteURL == "" {
		return nil, "", "", nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, l.remoteURL, nil)
	if err != nil {
		return nil, "", "", err
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", "", fmt.Errorf("unexpected status from domain-config url: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", "", err
	}
	if len(body) == 0 {
		return nil, "", "", fmt.Errorf("empty domain-config body from url")
	}

	cfg, err := decodeAndValidate(body)
	if err != nil {
		return nil, "", "", err
	}

	return cfg, "remote", l.remoteURL, nil
}

func (l *Loader) loadLocal() (*domain.SearchDomainConfig, string, string, error) {
	location := resolveLocalPath(l.localFile)
	if location == "" {
		return nil, "", "", fmt.Errorf("DOMAIN_CONFIG_LOCAL_FILE is not set")
	}

	body, err := os.ReadFile(location)
	if err != nil {
		return nil, "", "", fmt.Errorf("failed to load domain config [%s]: %w", location, err)
	}

	cfg, err := decodeAndValidate(body)
	if err != nil {
		return nil, "", "", err
	}

	return cfg, "local", location, nil
}

func decodeAndValidate(body []byte) (*domain.SearchDomainConfig, error) {
	var cfg domain.SearchDomainConfig
	if err := json.Unmarshal(body, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse domain config: %w", err)
	}

	if err := validate(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func validate(cfg *domain.SearchDomainConfig) error {
	if cfg == nil {
		return fmt.Errorf("domain config is null")
	}

	for className, fields := range cfg.Fields {
		if err := validateFields(className, "mainFields", fields.MainFields, true); err != nil {
			return err
		}
		if err := validateFields(className, "indexFields", fields.IndexFields, true); err != nil {
			return err
		}
		if err := validateFields(className, "jsonFields", fields.JSONFields, false); err != nil {
			return err
		}
	}
	return nil
}

func validateFields(className, group string, fields []domain.FieldConfig, requiresDB bool) error {
	for _, field := range fields {
		name := strings.TrimSpace(field.Name)
		if name == "" {
			return fmt.Errorf("invalid domain config: empty name in %s for class [%s]", group, className)
		}

		if requiresDB && strings.TrimSpace(field.DB) == "" {
			return fmt.Errorf("invalid domain config: empty db in %s for field [%s], class [%s]", group, name, className)
		}

		if strings.TrimSpace(field.Type) == "" {
			return fmt.Errorf("invalid domain config: empty type in %s for field [%s], class [%s]", group, name, className)
		}
		if _, err := domain.ParseColumnType(field.Type); err != nil {
			return fmt.Errorf("invalid domain config: unknown type [%s] in %s for field [%s], class [%s]",
				field.Type, group, name, className)
		}
	}
	return nil
}

func buildInfo(source, location string, cfg *domain.SearchDomainConfig) *domain.DomainConfigInfo {
	canonical, _ := json.Marshal(cfg)
	h := sha256.Sum256(canonical)

	return &domain.DomainConfigInfo{
		Source:   source,
		Location: location,
		Hash:     hex.EncodeToString(h[:]),
		LoadedAt: time.Now().UTC(),
		Config:   cfg,
	}
}

func resolveLocalPath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "classpath:") {
		return strings.TrimPrefix(trimmed, "classpath:")
	}
	if strings.HasPrefix(trimmed, "file://") {
		return strings.TrimPrefix(trimmed, "file://")
	}
	return trimmed
}
