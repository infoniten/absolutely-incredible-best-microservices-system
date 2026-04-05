# Quantara

Распределённая микросервисная платформа для управления данными.

## Быстрый старт

```bash
# Запуск всех сервисов одной командой
make up-build

# Или напрямую через docker-compose
docker-compose up -d --build
```

После запуска доступны:
- gRPC UI для ID Service: http://localhost:8080
- gRPC UI для GlobalID Service: http://localhost:8081
- gRPC UI для Lock Service: http://localhost:8082
- DataDictionary HTTP API: http://localhost:8083
- gRPC UI для DataDictionary: http://localhost:8084
- gRPC UI для Transaction Service: http://localhost:8085
- gRPC UI для Search Service: http://localhost:8086
- Jaeger UI (трейсинг): http://localhost:16686

## Архитектура

Система построена на микросервисной архитектуре, где каждый сервис отвечает за конкретную доменную функцию. Сервисы взаимодействуют через gRPC. Базовые инфраструктурные сервисы (генерация ID, блокировки) обеспечивают фундамент для сервисов более высокого уровня.

## Сервисы

### ID Generator Service
Генерация уникальных последовательных идентификаторов в рамках одного узла.
- Локальная генерация последовательностей
- Высокопроизводительное выделение ID

### GlobalID Generator Service
Генерация глобально уникальных идентификаторов в распределённой среде.
- Уникальные ID на уровне кластера
- Координация между узлами

### Lock Service
Механизм распределённых блокировок для синхронизации ресурсов.
- Захват и освобождение распределённых блокировок
- Таймауты и предотвращение deadlock

### Transaction Service
Управление транзакционной записью объектов в базу данных.
- Staged transactions: накопление объектов в Redis, атомарный commit в PostgreSQL
- 3 алгоритма сохранения: Embedded, Revisioned, DraftableDateBounded
- Поддержка до 100k+ объектов в транзакции
- gRPC API: `localhost:50054`

### Object Framework Service
Унифицированное управление жизненным циклом объектов.
- CRUD-операции над объектами
- Версионирование и история изменений

### DataDictionary Service
Управление метаданными и схемами доменных объектов.
- Регистрация и валидация схем
- Запросы к метаданным
- HTTP API: `localhost:8083`
- gRPC API: `localhost:9090`

### Search Service
Полнотекстовый и структурированный поиск.
- Управление индексами
- Выполнение запросов по источникам данных
- gRPC API: `localhost:50055`

## Статус

- [x] ID Generator Service
- [x] GlobalID Generator Service
- [x] Lock Service
- [x] Transaction Service
- [ ] Object Framework Service
- [x] DataDictionary Service
- [x] Search Service
