# enricher-service

REST-сервис обогащения объектов через вызовы в `search-service`.

## Что делает сервис

1. Находит root-объект:
   - по `globalId` через `search-service` endpoint `getObjectByGlobalId`
   - по списку revision `id` через `search-service` endpoint `getObjectRevisionByIds`
2. Рекурсивно обогащает запрошенные поля по связям:
   - `GLOBAL_LINK` (через `getObjectByGlobalId`)
   - `GLOBAL_ITEM` (через `getObjectByGlobalItem`)
   - `EMBEDDED_SET` (через `getObjectCollectionByParentId`)
3. Возвращает JSON-результат.

Конфигурация классов/полей/связей загружается из DataDictionary (`/api/search-service/metadata/v3`).

## Индексы metadata и разрешение `outputField`

Этот раздел описывает внутреннюю модель сервиса. Индексы строятся один раз при
создании Spring-компонентов и затем используются для валидации селекторов и
runtime-разрешения relations без повторного обхода всей metadata.

### Основные термины

- **Declared target class** — класс, указанный в metadata в `targetClass`
  relation. Это тип, известный до обращения в `search-service`.
- **Actual class** — фактический класс полученного объекта. Он определяется по
  полю `objectClass` в JSON-ответе `search-service`.
- **Parent/self** — сам класс и все его родители.
- **Descendant/self** — сам класс и все его наследники.
- **Полиморфная relation** — relation, declared target которой имеет
  наследников, поэтому фактический объект может относиться к разным классам.
  Например, `Trade.trader -> Actor` может вернуть `Person` или `Robot`. Если
  declared target конкретный, фактическим классом также может быть он сам.
- **Selector token relation** — `alias` для `GLOBAL_LINK` и `name` для остальных
  типов relation. Все токены и имена полей сравниваются после нормализации
  регистра и пробелов.

`ObjectClassInfo` используется как ключ внутренних map. Нужно использовать
канонические экземпляры из `ObjectClassRegistry`, а не создавать
`ObjectClassInfo` вручную.

`targetClass` может быть любым зарегистрированным классом: абстрактным,
конкретным классом с наследниками или конечным классом без наследников. Если
наследников нет, полиморфные индексы содержат только сам `targetClass` и
разрешение фактически становится обычным точным поиском.

### Последовательность построения

Индексы зависят друг от друга и создаются в следующем порядке:

1. `ObjectClassRegistry` индексирует классы.
2. `ObjectClassHierarchyRegistry` строит прямую и обратную иерархию.
3. `FieldRegistry` рассчитывает declared, inherited и polymorphic-наборы полей.
4. `RelationRegistry` рассчитывает direct, effective и polymorphic-relations.

После завершения этой последовательности запросы работают с готовыми map/set.

### `ObjectClassRegistry`

| Индекс | Ключ | Значение | Назначение |
| --- | --- | --- | --- |
| `byName` | canonical metadata name, например `ACTOR` | `ObjectClassInfo` | Поиск класса по metadata name |
| `bySource` | нормализованный `sourceValue`, например `actor` | `ObjectClassInfo` | Поиск класса по API/JSON-имени |

Метод `fromSourceValueOrName()` сначала ищет по `sourceValue`, затем по metadata
name. Он используется для входного `{objectClass}`, source-части `outputField` и
поля `objectClass` в ответах `search-service`.

### `ObjectClassHierarchyRegistry`

| Индекс | Содержимое | Назначение |
| --- | --- | --- |
| `parentsOrSelf` | `class -> Set(self + parents)` | Быстрая проверка `isParentOrSelf()` |
| `parentsOrSelfOrdered` | `class -> List(self, parent, ..., root)` | Разрешение inherited relation с приоритетом ближайшего класса |
| `descendantsOrSelfByClass` | `class -> Set(self + descendants)` | Полиморфная проверка полей и relations target-класса |

`descendantsOrSelfByClass` строится инвертированием metadata
`hierarchy.parentsOrSelf`. Порядок наследников не имеет значения. Metadata должна
содержать транзитивную цепочку родителей и сам класс.

Пример:

```text
Person -> [Person, Actor, RevisionedEntity]
Robot  -> [Robot, Actor, RevisionedEntity]
```

даёт обратный индекс:

```text
Actor -> {Actor, Person, Robot}
```

### `FieldRegistry`

Имена полей хранятся в `Set<String>`, поэтому одинаковое имя в независимых
ветках наследования сохраняется один раз. В объектной модели поле родителя не
переопределяется наследником, а одинаковые поля в независимых ветках имеют один
тип.

| Индекс | Содержимое | Назначение |
| --- | --- | --- |
| `declaredFieldsByClass` | Только поля, объявленные классом | Проверка корневого `source.field` |
| `fieldsInHierarchyByClass` | Declared-поля класса и всех родителей | Effective-поля конкретного actual-класса |
| `polymorphicFieldsByClass` | Объединение effective-полей класса и всех наследников | Проверка поля после перехода по relation |

В `declaredFieldsByClass` также добавляются неявные поля `id`, `globalId` и
`objectClass`.

Для `Actor <- Person <- Employee` индексы выглядят концептуально так:

```text
declared(Actor)    = {name}
declared(Person)   = {firstName}
declared(Employee) = {employeeNumber}

effective(Employee) = {name, firstName, employeeNumber}
polymorphic(Actor)   = {name, firstName, employeeNumber, ...поля других subtype}
```

`hasFieldInPolymorphicHierarchy(Actor, "firstName")` возвращает `true`, даже
если поле объявлено только у `Person`. Это означает «селектор возможен хотя бы
для одного subtype», а не «поле есть у каждого Actor».

### `RelationRegistry`

`RelationDef` содержит тип relation, selector token, source/id/role metadata,
declaring class и target class.

| Индекс | Форма | Назначение |
| --- | --- | --- |
| `bySourceClassToken` | `declaringClass -> token -> RelationDef` | Relation, объявленные непосредственно классом |
| `bySourceClassTokenWithParents` | `actualClass -> token -> RelationDef` | Effective-relations actual-класса с inherited relations |
| `polymorphicRelations` | `baseClass -> token -> actualClass -> RelationDef` | Все допустимые варианты nested relation для класса и его наследников |

`bySourceClassTokenWithParents` строится по
`parentsOrSelfOrdered(actualClass)`: сначала actual-класс, затем родители. Если
metadata содержит одинаковый token у наследника и предка, сервис пишет `WARN` и
использует definition наследника. Такая metadata считается нежелательной, но не
блокирует запуск.

Одинаковый token у sibling-классов разрешён. Target и остальные параметры
relation могут различаться, поэтому sibling-relations нельзя сворачивать в одну
`RelationDef`.

Вложенная relation может быть объявлена в любом из трёх мест:

- у родителя declared target — тогда target и его наследники получают её по
  наследованию;
- непосредственно у declared target;
- у одного или нескольких наследников declared target — тогда она доступна
  только для соответствующих actual-классов и их наследников.

Таким образом, выражение «relation наследника» не означает, что nested relations
обязаны объявляться только у наследников. Это один из допустимых вариантов.

Пример:

```text
RevisionedEntity
├── Actor.alternativeIdentifiers
│   -> ActorAlternativeIdentifier
├── Counterparty.alternativeIdentifiers
│   -> CounterpartyAlternativeIdentifier
└── LegalEntity.alternativeIdentifiers
    -> LegalEntityAlternativeIdentifier
```

Полиморфный индекс сохраняет все варианты:

```text
RevisionedEntity + alternativeIdentifiers
  Actor        -> Actor.alternativeIdentifiers
  Counterparty -> Counterparty.alternativeIdentifiers
  LegalEntity  -> LegalEntity.alternativeIdentifiers
```

На runtime конкретная definition выбирается по actual `objectClass`.

### Валидация `outputField`

`prepareSelectors()` разбирает список `outputField` в дерево `SelectorNode` и
валидирует его один раз. Для revision endpoint то же дерево переиспользуется для
всех объектов batch.

Для селектора:

```text
Source.relation1.relation2.field
```

валидация выполняется последовательно:

1. `Source` разрешается через `ObjectClassRegistry` и проверяется относительно
   root-класса запроса.
2. На root-уровне поле ищется только в `declaredFieldsByClass`, а relation — в
   `bySourceClassToken`.
3. После перехода по relation её `targetClass` становится корнем следующего
   полиморфного шага.
4. Nested relation ищется через `polymorphicRelations`: результатом может быть
   несколько `RelationDef`, привязанных к разным actual-классам.
5. Target-классы всех найденных вариантов становятся корнями следующего шага.
6. Конечное поле проверяется через `polymorphicFieldsByClass` для этих target.

Путь считается валидным, если существует хотя бы один возможный actual-класс,
для которого его можно пройти полностью. Иными словами, необязательно, чтобы
поле или nested relation присутствовали у каждого наследника declared target.

Например:

```text
relation1 -> RevisionedEntity
relation2 -> alternativeIdentifiers
field     -> ola
```

Если `ola` объявлено только в `LegalEntityAlternativeIdentifier`, селектор всё
равно валиден. Для `LegalEntity` будет возвращено значение, а для `Actor` или
`Counterparty` — `null`.

Селектор считается невалидным, если:

- source неизвестен или не относится к иерархии root-класса;
- конечное поле отсутствует во всех допустимых ветках;
- промежуточный сегмент не является relation ни в одной допустимой ветке;
- запрошен неподдержанный тип relation;
- одновременно запрошены relation целиком и её вложенные поля.

Текущая реализация не возвращает ошибку клиенту для таких `outputField`.
Невалидный селектор пропускается, в лог пишется `Skipping invalid outputField`
или `Skipping conflicting outputField`, а остальные валидные селекторы
обрабатываются дальше. Если все переданные `outputField` невалидны, ответ
будет содержать только служебное поле `objectClass`.

Сейчас runtime-обогащение поддерживает `GLOBAL_LINK`, `GLOBAL_ITEM` и
`EMBEDDED_SET`. Остальные типы могут присутствовать в metadata, но их запрос
через enricher отклоняется.

### Runtime-разрешение и построение результата

После статической валидации сервис загружает связанные объекты рекурсивно:

1. Из JSON связанного объекта читается `objectClass`.
2. `objectClass` преобразуется в канонический `ObjectClassInfo`. Если поле
   отсутствует или содержит неизвестный класс, сервис не может определить
   subtype и использует declared `targetClass` relation как fallback.
3. Nested relation выбирается через effective-индекс
   `bySourceClassTokenWithParents[actualClass][token]`.
4. Объект загружается из `search-service` в соответствии с типом `RelationDef`.
5. Для загруженного объекта снова определяется actual-класс, после чего
   обработка продолжается рекурсивно.

Если селектор валиден для одного subtype, но фактический объект относится к
другому subtype, отсутствующее поле или nested relation возвращается как `null`.
Для `EMBEDDED_SET` проекция строится отдельно для каждого элемента.

Если селектор валиден по metadata, но самого поля нет в JSON фактического
объекта, в результате также возвращается `null`.

Корректный `objectClass` в ответе `search-service` критичен для выбора sibling
relation с одинаковым token и разными target.

### Request-scoped caches

Помимо metadata-индексов, `EnrichmentService.RuntimeContext` содержит кеши на
время одного запроса:

| Кеш | Ключ | Что предотвращает |
| --- | --- | --- |
| `globalCache` | `objectClass + globalId` | Повторный `GLOBAL_LINK` запрос |
| `globalItemCache` | target/idField/roleField/role/relationGlobalId | Повторный `GLOBAL_ITEM` запрос |
| `parentCache` | `objectClass + parentId` | Повторный `EMBEDDED_SET` запрос |

Кеши не разделяются между HTTP-запросами и не могут содержать устаревшие данные
между запросами.

## Endpoint

`GET /api/v1/enriched-objects/{objectClass}`

`POST /api/v1/enriched-objects/{objectClass}/revisions`

### Query params

- `globalId` (required) — globalId объекта.
- `outputField` (optional, repeatable) — список полей/путей в формате `source.path`.

Для endpoint по revision `id`:

- список `id` передаётся JSON-массивом в теле запроса;
- `outputField` (optional, repeatable) — список полей/путей в формате `source.path`.

Если `outputField` не передан, возвращается полный JSON объекта из `search-service`.

## Примеры

### Полный объект (без outputField)

```bash
curl "http://localhost:8089/api/v1/enriched-objects/FxSpotForwardTrade?globalId=123"
```

### Полные объекты по нескольким revision id

```bash
curl -X POST \
  "http://localhost:8089/api/v1/enriched-objects/FxSpotForwardTrade/revisions" \
  -H "Content-Type: application/json" \
  -d '[12345, 12346, 12347]'
```

Ответ — JSON-массив найденных и обогащённых ревизий:

```json
[
  {
    "objectClass": "FxSpotForwardTrade",
    "id": 12345,
    "contractId": 100500
  },
  {
    "objectClass": "FxSpotForwardTrade",
    "id": 12346,
    "contractId": 100501
  }
]
```

Проекция применяется к каждому объекту массива:

```bash
curl -X POST \
  "http://localhost:8089/api/v1/enriched-objects/FxSpotForwardTrade/revisions?outputField=Trade.id&outputField=Trade.counterparty.name" \
  -H "Content-Type: application/json" \
  -d '[12345, 12346]'
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

## Поиск связи GLOBAL_ITEM

`GLOBAL_ITEM` разрешается автоматически, когда один из `outputField` указывает на такую связь. Enricher вызывает следующий endpoint `search-service`:

`GET /api/v2/objects/{objectClass}/global-item`

Параметры запроса:

- `objectClass` — целевой класс связанного объекта из metadata relation;
- `relationGlobalId` — значение идентификатора родительского объекта;
- `idFieldName` — JSON-имя поля с идентификатором родительского объекта;
- `roleFieldName` — JSON-имя поля роли;
- `role` — значение роли связи.

Пример внутреннего запроса к `search-service`:

```bash
curl "http://localhost:8081/api/v2/objects/FxSpotForwardTrade/global-item?relationGlobalId=123456789&idFieldName=productId&roleFieldName=roleInProduct&role=MAIN"
```

Имена полей и значение роли берутся из metadata: `jsonIdFieldName`, `jsonRoleFieldName` и `roleValue`. Результат поиска включается в итоговый JSON как вложенный объект.

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

`targetClass` relation может быть любым зарегистрированным классом. Если у него
есть наследники, вложенный `outputField` проверяется для `targetClass` и всей его
иерархии. Например, если `Trade.trader` указывает на `Actor`, а `firstName`
объявлено в `Person extends Actor`, селектор `Trade.trader.firstName` валиден.
Для фактического `Person` возвращается значение, а для subtype без `firstName` —
`null`.

Вложенная relation может быть объявлена у родителя `targetClass`, непосредственно
у `targetClass` или у его наследника. Если sibling-классы используют одинаковый
selector token с разными target-классами, конкретная relation выбирается по
фактическому `objectClass`. Совпадение token у предка и наследника логируется как
metadata warning; definition наследника имеет приоритет.

## Формат результата

- Корневые поля возвращаются на верхнем уровне (`id`, `contractId`, и т.д.).
- Связанные сущности возвращаются вложенным JSON/массивом.
- `objectClass` всегда возвращается в ответе при проекции (`outputField` задан).

## Конфигурация

Для batch-контракта `src/main/resources/application.yml` должен содержать:

```yaml
server:
  port: 8089

enricher:
  search-service:
    base-url: http://localhost:8081
    global-endpoint: /api/objects/{objectClass}?globalId={globalId}
    revision-endpoint: /api/objects/{objectClass}/revisions
    parent-endpoint: /api/objects/{objectClass}/parent/{parentId}
    global-item-endpoint: /api/v2/objects/{objectClass}/global-item?relationGlobalId={relationGlobalId}&idFieldName={idFieldName}&roleFieldName={roleFieldName}&role={role}
  metadata:
    url: http://localhost:8083/api/search-service/metadata/v3
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
- `enricher.search.global_item.count`
- `enricher.search.global_item.errors`
- `enricher.search.global_item.duration`
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
  - `ENRICHER_SEARCH_SERVICE_GLOBAL_ITEM_ENDPOINT`
  - `ENRICHER_METADATA_URL`
