# Order Service

Микросервис для обработки заказов с использованием Go, PostgreSQL, Kafka и in-memory кеширования.

## Архитектура

Сервис состоит из следующих компонентов:

- **HTTP API** - RESTful API для получения информации о заказах
- **Kafka Consumer** - обработчик сообщений из брокера Kafka
- **PostgreSQL** - основное хранилище данных
- **In-Memory Cache** - кеш с LRU политикой вытеснения для быстрого доступа к заказам
- **Web Interface** - простой веб-интерфейс для просмотра заказов

## Быстрый старт

### Предварительные требования

- Docker и Docker Compose
- Go 1.24.5 или выше

### Установка и запуск

1. **Клонирование репозитория**
```bash
git clone https://github.com/darkus079/OrderService/tree/develop
```

2. **Запуск инфраструктуры (PostgreSQL, Kafka)**
```bash
docker-compose up -d
```

3. **Запуск приложения**
```bash
go run cmd/server/main.go
```

Сервис будет доступен по адресу: `http://localhost:8081`

### Остановка

```bash
docker-compose down
# или
docker-compose down -v
```

## Конфигурация

Сервис настраивается через переменные окружения:

### Server
- `SERVER_HOST` - хост сервера (по умолчанию: localhost)
- `SERVER_PORT` - порт сервера (по умолчанию: 8081)

### Database
- `DB_HOST` - хост PostgreSQL (по умолчанию: localhost)
- `DB_PORT` - порт PostgreSQL (по умолчанию: 5432)
- `DB_USER` - пользователь БД (по умолчанию: orderservice)
- `DB_PASSWORD` - пароль БД (по умолчанию: password)
- `DB_NAME` - имя БД (по умолчанию: orderservice)
- `DB_SSLMODE` - режим SSL (по умолчанию: disable)

### Kafka
- `KAFKA_BROKERS` - брокеры Kafka (по умолчанию: localhost:9092)
- `KAFKA_TOPIC` - топик для заказов (по умолчанию: orders)
- `KAFKA_GROUP_ID` - группа consumer'а (по умолчанию: order-service)

### Cache
- `CACHE_MAX_SIZE` - максимальный размер кеша (по умолчанию: 1000)

## API Endpoints

### Получение заказа
```http
GET /order/{order_uid}
```

Возвращает информацию о заказе по его уникальному идентификатору.

**Пример ответа:**
```json
{
    "order_uid": "b563feb7b2b84b6test",
    "track_number": "WBILMTESTTRACK",
    "entry": "WBIL",
    "delivery": {
      "name": "Test Testov",
      "phone": "+9720000000",
      "zip": "2639809",
      "city": "Kiryat Mozkin",
      "address": "Ploshad Mira 15",
      "region": "Kraiot",
      "email": "test@gmail.com"
    },
    "payment": {
      "transaction": "b563feb7b2b84b6test",
      "request_id": "",
      "currency": "USD",
      "provider": "wbpay",
      "amount": 1817,
      "payment_dt": 1637907727,
      "bank": "alpha",
      "delivery_cost": 1500,
      "goods_total": 317,
      "custom_fee": 0
    },
    "items": [
      {
        "chrt_id": 9934930,
        "track_number": "WBILMTESTTRACK",
        "price": 453,
        "rid": "ab4219087a764ae0btest",
        "name": "Mascaras",
        "sale": 30,
        "size": "0",
        "total_price": 317,
        "nm_id": 2389212,
        "brand": "Vivienne Sabo",
        "status": 202
      }
    ],
    "locale": "en",
    "internal_signature": "",
    "customer_id": "test",
    "delivery_service": "meest",
    "shardkey": "9",
    "sm_id": 99,
    "date_created": "2021-11-26T06:22:19Z",
    "oof_shard": "1"
  }
```

### Проверка состояния сервиса
```http
GET /health
```

Проверка состояния сервиса.

**Пример ответа:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z",
  "cache_size": 42
}
```

### Статистика
```http
GET /stats
```

Получение статистики сервиса.

**Пример ответа:**
```json
{
  "cache_size": 42,
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### Веб-интерфейс
```http
GET /
```

Простой веб-интерфейс для просмотра заказов.

## Тестирование

### Подготовка к тестированию

1. **Убедитесь, что инфраструктура запущена**
```bash
docker-compose up -d
```

2. **Дождитесь готовности сервисов**
```bash
docker-compose exec postgres pg_isready -U orderservice -d orderservice
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Запуск тестов

#### Все тесты
```bash
go test -v ./...

# Запуск с отчетом о покрытии
go test -v -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

#### Unit тесты
```bash
# Тесты моделей
go test -v ./internal/models/...

# Тесты кеша
go test -v ./internal/cache/...

# Тесты handlers
go test -v ./internal/handlers/...

# Тесты Kafka consumer
go test -v ./internal/kafka/...

# Тесты PostgreSQL repository
go test -v ./internal/database/...
```

#### Интеграционные тесты
```bash
# Интеграционные тесты
go test -v -tags=integration ./tests/integration/...
```

#### Тесты крайних случаев
```bash
go test -v ./tests/edge_cases/...
```

### Очистка тестовых данных

Тесты автоматически очищают данные после выполнения. Для ручной очистки:

```bash
# Очистка базы данных
docker-compose exec postgres psql -U orderservice -d orderservice -c "TRUNCATE TABLE orders;"

# Очистка топика Kafka
docker-compose restart kafka
```

## Отладка

### Просмотр логов

```bash
# Логи PostgreSQL
docker-compose logs postgres

# Логи Kafka
docker-compose logs kafka

# Логи всех сервисов
docker-compose logs -f
```

### Мониторинг Kafka

Kafka UI доступен по адресу: `http://localhost:8080`

### Подключение к базе данных

```bash
docker-compose exec postgres psql -U orderservice -d orderservice

# Просмотр таблиц
\dt

# Просмотр заказов
SELECT order_uid, customer_id, date_created FROM orders LIMIT 10;
```

## Отправка тестового сообщения в Kafka

Для тестирования можно отправить сообщение в Kafka:

```bash
docker-compose exec kafka bash

kafka-console-producer --broker-list localhost:9092 --topic orders << EOF
{
  "order_uid": "test123456789",
  "track_number": "TEST_TRACK_001",
  "entry": "WBIL",
  "delivery": {
    "name": "John Doe",
    "phone": "+1234567890",
    "zip": "12345",
    "city": "Test City",
    "address": "123 Test Street",
    "region": "Test Region",
    "email": "test@example.com"
  },
  "payment": {
    "transaction": "test_transaction_001",
    "request_id": "",
    "currency": "USD",
    "provider": "test_provider",
    "amount": 1000,
    "payment_dt": 1640995200,
    "bank": "test_bank",
    "delivery_cost": 500,
    "goods_total": 500,
    "custom_fee": 0
  },
  "items": [
    {
      "chrt_id": 123456,
      "track_number": "TEST_TRACK_001",
      "price": 500,
      "rid": "test_rid_001",
      "name": "Test Item",
      "sale": 0,
      "size": "M",
      "total_price": 500,
      "nm_id": 654321,
      "brand": "Test Brand",
      "status": 202
    }
  ],
  "locale": "en",
  "internal_signature": "",
  "customer_id": "test_customer",
  "delivery_service": "test_service",
  "shardkey": "1",
  "sm_id": 1,
  "date_created": "2024-01-15T10:00:00Z",
  "oof_shard": "1"
}
EOF
```

После отправки сообщения проверьте, что заказ появился в системе:

```bash
curl http://localhost:8081/order/test123456789
```

## Структура проекта

```
OrderService/
├── cmd/
│   └── server/          # Точка входа приложения
│       └── main.go
├── internal/            # Внутренняя логика приложения
│   ├── cache/          # In-memory кеш
│   ├── config/         # Конфигурация
│   ├── database/       # Работа с PostgreSQL
│   ├── handlers/       # HTTP обработчики
│   ├── kafka/          # Kafka consumer
│   └── models/         # Модели данных
├── tests/              # Тесты
│   ├── integration/    # Интеграционные тесты
│   ├── edge_cases/     # Тесты крайних случаев
│   └── testdata/       # Тестовые данные
├── web/
│   └── templates/      # Web интерфейс
├── docker-compose.yml  # Docker окружение
├── go.mod             # Go модули
└── README.md          # Документация
```
