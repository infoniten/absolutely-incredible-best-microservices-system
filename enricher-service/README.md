# enricher-service

REST-сервис обогащения объектов через вызовы в `search-service`.

## Что делает сервис

1. Находит root-объект:
   - по `globalId` через `search-service` endpoint `getObjectByGlobalId`
   - по `id` через `search-service` endpoint `getObjectRevisionById`
2. Рекурсивно обогащает запрошенные поля по связям:
   - `GLOBAL_LINK` (через `getObjectByGlobalId`)
   - `EMBEDDED_SET` (через `getObjectCollectionByParentId`)
3. Возвращает JSON-результат.

Конфигурация классов/полей/связей загружается из DataDictionary (`/api/search-service/metadata/v2`).

## Endpoint

`GET /api/v1/enriched-objects/{objectClass}`

`GET /api/v1/enriched-objects/{objectClass}/revisions/{id}`

### Query params

- `globalId` (required) — globalId объекта.
- `outputField` (optional, repeatable) — список полей/путей в формате `source.path`.

Для endpoint по `id`:

- `id` передаётся в path (`/revisions/{id}`).
- `outputField` (optional, repeatable) — список полей/путей в формате `source.path`.

Если `outputField` не передан, возвращается полный JSON объекта из `search-service`.

## Примеры

### Полный объект (без outputField)

```bash
curl "http://localhost:8089/api/v1/enriched-objects/FxSpotForwardTrade?globalId=123"
```

### Полный объект по revision id (без outputField)

```bash
curl "http://localhost:8089/api/v1/enriched-objects/FxSpotForwardTrade/revisions/12345"
```

### Выборка отдельных полей

```bash
curl "http://localhost:8089/api/v1/enriched-objects/FxSpotForwardTrade?globalId=123&outputField=Trade.id&outputField=Trade.contractId"
```

Пример ответа:

```json
{
  "objectClass": "FxSpotForwardTrade",
  "id": 12345,
  "contractId": 100500
}
```

### Выборка с вложенной связанной сущностью

```bash
curl "http://localhost:8089/api/v1/enriched-objects/FxSpotForwardTrade?globalId=123&outputField=Trade.counterparty.name&outputField=Trade.counterparty.id"
```

Пример ответа:

```json
{
  "objectClass": "FxSpotForwardTrade",
  "counterparty": {
    "name": "ACME BANK",
    "id": 9988
  }
}
```

## Формат результата

- Корневые поля возвращаются на верхнем уровне (`id`, `contractId`, и т.д.).
- Связанные сущности возвращаются вложенным JSON/массивом.
- `objectClass` всегда возвращается в ответе при проекции (`outputField` задан).

## Конфигурация

`src/main/resources/application.yml`:

```yaml
server:
  port: 8089

enricher:
  search-service:
    base-url: http://localhost:8081
    global-endpoint: /api/objects/{objectClass}?globalId={globalId}
    revision-endpoint: /api/objects/{objectClass}/revisions/{id}
    parent-endpoint: /api/objects/{objectClass}/parent/{parentId}
  metadata:
    url: http://localhost:8083/api/search-service/metadata/v2
    local-file: ""
```

## Метрики

Actuator endpoint:

- `GET /actuator/prometheus`

Кастомные метрики сервиса:

- `enricher.enrich.count`
- `enricher.enrich.errors`
- `enricher.enrich.duration`
- `enricher.projection.depth.count`
- `enricher.relation.resolve.count`
- `enricher.search.global.count`
- `enricher.search.global.errors`
- `enricher.search.global.duration`
- `enricher.search.revision.count`
- `enricher.search.revision.errors`
- `enricher.search.revision.duration`
- `enricher.search.parent.count`
- `enricher.search.parent.errors`
- `enricher.search.parent.duration`

Основные теги:

- `object_class`
- `mode` (`full` или `projection`)
- `output_fields_size`
- `depth`
- `relation_type`

## Локальная сборка

```bash
cd enricher-service
mvn -DskipTests package
```

## Docker

Сервис уже подключен в корневой `docker-compose.yaml`:

- порт: `8089`
- переменные:
  - `ENRICHER_SEARCH_SERVICE_BASE_URL`
  - `ENRICHER_SEARCH_SERVICE_GLOBAL_ENDPOINT`
  - `ENRICHER_SEARCH_SERVICE_REVISION_ENDPOINT`
  - `ENRICHER_SEARCH_SERVICE_PARENT_ENDPOINT`
  - `ENRICHER_METADATA_URL`
