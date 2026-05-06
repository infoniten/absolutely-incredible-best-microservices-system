# enricher-service

REST-сервис обогащения объектов через вызовы в `search-service`.

## Что делает сервис

1. Находит root-объект по `globalId` через `search-service` endpoint `getObjectByGlobalId`.
2. Рекурсивно обогащает запрошенные поля по связям:
   - `GLOBAL_LINK` (через `getObjectByGlobalId`)
   - `EMBEDDED_SET` (через `getObjectCollectionByParentId`)
3. Возвращает JSON-результат.

Конфигурация классов/полей/связей загружается из DataDictionary (`/api/search-service/metadata/v2`).

## Endpoint

`GET /api/v1/enriched-objects/{objectClass}`

### Query params

- `globalId` (required) — globalId объекта.
- `outputField` (optional, repeatable) — список полей/путей в формате `source.path`.

Если `outputField` не передан, возвращается полный JSON объекта из `search-service`.

## Примеры

### Полный объект (без outputField)

```bash
curl "http://localhost:8089/api/v1/enriched-objects/FxSpotForwardTrade?globalId=123"
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
    parent-endpoint: /api/objects/{objectClass}/parent/{parentId}
  metadata:
    url: http://localhost:8083/api/search-service/metadata/v2
    local-file: ""
```

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
  - `ENRICHER_SEARCH_SERVICE_PARENT_ENDPOINT`
  - `ENRICHER_METADATA_URL`

