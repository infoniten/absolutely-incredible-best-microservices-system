# Quantara

Распределённая микросервисная платформа для управления данными и обработки торговых операций.

## Быстрый старт

```bash
# Запуск всех сервисов одной командой
make up-build

# Или напрямую через docker-compose
docker-compose up -d --build
```

После запуска доступны:

| Сервис | URL | Описание |
|--------|-----|----------|
| gRPC UI для ID Service | http://localhost:8080 | Генерация локальных ID |
| gRPC UI для GlobalID Service | http://localhost:8081 | Генерация глобальных ID |
| gRPC UI для Lock Service | http://localhost:8082 | Распределённые блокировки |
| DataDictionary HTTP API | http://localhost:8083 | Управление метаданными |
| gRPC UI для DataDictionary | http://localhost:8084 | gRPC интерфейс |
| gRPC UI для Transaction Service | http://localhost:8085 | Транзакции |
| gRPC UI для Search Service | http://localhost:8086 | Поиск объектов |
| Object Framework Health | http://localhost:8088/health | Health check |
| Object Framework Metrics | http://localhost:8088/metrics | Метрики |
| Jaeger UI | http://localhost:16686 | Распределённый трейсинг |
| Kafka | localhost:29092 | Message broker |

## Архитектура

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              External Systems                                │
│                                    │                                         │
│                                    ▼                                         │
│                           ┌───────────────┐                                  │
│                           │     Kafka     │                                  │
│                           │  moex.trades  │                                  │
│                           └───────┬───────┘                                  │
│                                   │                                          │
│                                   ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                      Object Framework Service                        │    │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐ │    │
│  │  │  Kafka   │→ │  Parser  │→ │ Processor│→ │ Transaction Manager  │ │    │
│  │  │ Consumer │  │  (MOEX)  │  │ (FxSpot) │  │                      │ │    │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────────────────┘ │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                    │           │           │           │                     │
│          ┌────────┴───┐  ┌────┴────┐  ┌───┴───┐  ┌───┴────┐                 │
│          ▼            ▼  ▼         ▼  ▼       ▼  ▼        ▼                 │
│  ┌────────────┐ ┌────────────┐ ┌─────────┐ ┌─────────┐ ┌─────────────┐      │
│  │ ID Service │ │ GlobalID   │ │  Lock   │ │ Search  │ │ Transaction │      │
│  │   :50051   │ │  Service   │ │ Service │ │ Service │ │   Service   │      │
│  └────────────┘ │   :50052   │ │  :50053 │ │  :50055 │ │   :50054    │      │
│                 └────────────┘ └─────────┘ └─────────┘ └─────────────┘      │
│                        │             │           │             │             │
│                        └─────────────┴───────────┴─────────────┘             │
│                                          │                                   │
│                                          ▼                                   │
│                               ┌─────────────────────┐                        │
│                               │     PostgreSQL      │                        │
│                               │       + Redis       │                        │
│                               └─────────────────────┘                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Сервисы

### Object Framework Service (NEW)
Обработка входящих сообщений MOEX и создание торговых объектов.

**Функционал:**
- Потребление XML сообщений из Kafka топика `moex.trades`
- Парсинг MOEX XML формата (ASTS)
- Создание/обновление FxSpotExchangeProduct, FxSpotForwardTrade, TradeCashflow
- Версионирование и ревизионирование объектов
- Идемпотентная обработка (дедупликация по messageID)
- Распределённые блокировки для конкурентного доступа

**Endpoints:**
- `GET /health` - полная проверка здоровья (DB + Kafka)
- `GET /health/live` - liveness probe для Kubernetes
- `GET /health/ready` - readiness probe для Kubernetes
- `GET /metrics` - метрики обработки сообщений

**Конфигурация:**
```bash
HTTP_PORT=8088
KAFKA_BROKERS=kafka:9092
KAFKA_TOPIC=moex.trades
KAFKA_CONSUMER_GROUP=object-framework-group
DATABASE_URL=postgres://...
ID_SERVICE_ADDR=id-service:50051
GLOBALID_SERVICE_ADDR=globalid-service:50052
LOCK_SERVICE_ADDR=lock-service:50053
SEARCH_SERVICE_ADDR=search-service:50055
TRANSACTION_SERVICE_ADDR=transaction-service:50054
LOCK_TTL_MS=30000
```

### ID Generator Service
Генерация уникальных последовательных идентификаторов в рамках одного узла.
- gRPC API: `localhost:50051`

### GlobalID Generator Service
Генерация глобально уникальных идентификаторов в распределённой среде.
- gRPC API: `localhost:50052`

### Lock Service
Механизм распределённых блокировок для синхронизации ресурсов.
- gRPC API: `localhost:50053`

### Transaction Service
Управление транзакционной записью объектов в базу данных.
- Staged transactions: накопление объектов в Redis, атомарный commit в PostgreSQL
- 3 алгоритма сохранения: Embedded, Revisioned, DraftableDateBounded
- Поддержка до 100k+ объектов в транзакции
- gRPC API: `localhost:50054`
- Metrics: `localhost:8087`

### Search Service
Полнотекстовый и структурированный поиск.
- Typed Filter AST для построения SQL запросов
- Кэширование в Redis
- gRPC API: `localhost:50055`

### DataDictionary Service
Управление метаданными и схемами доменных объектов.
- HTTP API: `localhost:8083`
- gRPC API: `localhost:9090`

## Отправка тестового сообщения

```bash
# Отправить тестовое MOEX сообщение в Kafka
docker exec -i quantara-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic moex.trades < test_message.txt

# Проверить логи обработки
docker logs -f quantara-object-framework

# Проверить метрики
curl http://localhost:8088/metrics
```

## Разработка

### Запуск тестов

```bash
# Unit тесты
cd object-framework
go test -v -short ./...

# Интеграционные тесты (требуют Docker)
go test -v -tags=integration ./test/integration/...
```

### Структура Object Framework

```
object-framework/
├── cmd/main.go                    # Entry point
├── internal/
│   ├── client/                    # gRPC клиенты к другим сервисам
│   ├── config/                    # Конфигурация из ENV
│   ├── domain/                    # Доменные модели
│   ├── kafka/                     # Kafka consumer
│   ├── parser/                    # MOEX XML парсер
│   ├── processor/                 # Бизнес-логика обработки
│   ├── repository/                # PostgreSQL репозитории
│   ├── server/                    # HTTP health server
│   └── telemetry/                 # OpenTelemetry
├── proto/                         # gRPC proto файлы
├── test/
│   ├── fixtures/                  # Тестовые XML файлы
│   ├── integration/               # Интеграционные тесты
│   └── testcontainers/           # Docker контейнеры для тестов
├── Dockerfile
├── Makefile
└── go.mod
```

## Поток обработки сообщения

```
1. Kafka Consumer получает XML сообщение из топика moex.trades
                    │
                    ▼
2. Генерируется уникальный messageID: topic:partition:offset:timestamp
                    │
                    ▼
3. Сообщение сохраняется в raw_messages со статусом PROCESSING
                    │
                    ▼
4. XML парсится, извлекается TRADENO (ID сделки в MOEX)
                    │
                    ▼
5. Резолвится GlobalID для сделки (get or create в globalid_mappings)
                    │
                    ▼
6. Захватывается распределённая блокировка по GlobalID
                    │
                    ▼
7. Бизнес-логика:
   - Get/Create FxSpotExchangeProduct (версионирование)
   - Get/Create FxSpotForwardTrade (маппинг из XML)
   - Create TradeCashflow (RECEIVE + PAY)
                    │
                    ▼
8. Transaction Service: BeginTransaction → Save → Commit
                    │
                    ▼
9. Освобождается блокировка
                    │
                    ▼
10. Обновляется статус в raw_messages: SAVED или FAILED
```

## Статус

- [x] ID Generator Service
- [x] GlobalID Generator Service
- [x] Lock Service
- [x] Transaction Service
- [x] Search Service
- [x] DataDictionary Service
- [x] Object Framework Service
- [x] Kafka Integration
- [x] Health Checks
- [x] Integration Tests
