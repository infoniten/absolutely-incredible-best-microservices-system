# Search Service (Go + gRPC)

- язык Go;
- транспорт gRPC;
- PostgreSQL + Redis;
- OpenTelemetry tracing (OTLP HTTP).

## Структура

- `cmd/main.go` — входная точка, DI и запуск gRPC.
- `internal/domainconfig` — загрузка/валидация domain-config (remote/local) + hash/loadedAt.
- `internal/registry` — реестры классов и иерархии.
- `internal/repository` — SQL-слой поиска.
- `internal/cache` — Redis cache.
- `internal/filter` — Typed Filter AST -> SQL builder + JOIN planning.
- `internal/service` — бизнес-логика поиска.
- `internal/server` — gRPC handlers.
- `proto/search_service.proto` — gRPC контракт.
- `domain-config.json` — локальный fallback domain-config.

## Поддержанные RPC

- `GetObjectRevisionById`
- `GetObjectByGlobalId`
- `GetObjectCollectionByParentId`
- `GetObjectGlobalIdByAltId`
- `GetObjectCollectionByFilter`
- `GetDomainConfig`

## Фильтрация (Typed AST)

Используется typed AST:
- `FilterExpr` (`and` / `or` / `not` / `predicate`)
- `FilterPredicate` (`field`, `operator`, `values`)
- `FilterValue` (`string/int/bool/date/datetime`)
- `SortField` (`field`, `direction`)

Селектор поля в фильтре: только `source.field`.
Если поле не найдено в main/index колонках, fallback в JSON (`data.content`) работает только при `SEARCH_FILTER_JSON_FIELD_ENABLED=true` и только для полей, явно объявленных в `jsonFields` у указанного `source`.

## Конфигурация (ENV)

- `GRPC_PORT` (default: `50055`)
- `DATABASE_URL` (default: `postgres://postgres:postgres@localhost:5432/quantara?sslmode=disable`)
- `DB_SCHEMA` (default: `murex`; если в metadata таблицы без схемы, сервис автоматически квалифицирует их как `<DB_SCHEMA>.<table>`)
- `REDIS_URL` (default: `redis://localhost:6379/0`)
- `DATADICTIONARY_URL` (optional, e.g. `http://datadictionary:8080` in docker network or `http://localhost:8083` locally)
- `DOMAIN_CONFIG_URL` (optional, overrides DataDictionary URL; default target is `.../api/search-service/metadata`)
- `DOMAIN_CONFIG_LOCAL_FILE` (default: `domain-config.json`)
- `SEARCH_FILTER_JSON_FIELD_ENABLED` (default: `false`)
- `JAEGER_ENDPOINT` (default: `localhost:4318`)
- `SERVICE_NAME` (default: `search-service`)

## Локальный запуск

```bash
cd search-service
make proto
go build -o bin/search-service ./cmd
./bin/search-service
```

По умолчанию сервис пытается получить metadata из DataDictionary endpoint:
`GET /api/search-service/metadata`.
Если endpoint недоступен или возвращает ошибку, используется локальный `domain-config.json`.

## Совместимость с transaction-service

- `transaction-service` пишет данные в PostgreSQL в таблицы `murex.<main/data/index>`.
- `search-service` читает те же таблицы; если в metadata таблицы без схемы, автоматически подставляется `DB_SCHEMA` (по умолчанию `murex`).
- `transaction-service` использует Redis только как staging для открытых транзакций (`{txId}:...`) и очищает эти ключи после `Commit`.
- Ключи кэша чтения `search-service` (`txn:committed:*`, `alt:*`) в `transaction-service` не формируются, поэтому поиск всегда корректно делает fallback в PostgreSQL.

## Docker сборка

```bash
cd search-service
docker build -t search-service:latest .
```
