# Order Service

Микросервис для обработки заказов с использованием Kafka, PostgreSQL и кэширования в памяти.

## Описание

Order Service - это демонстрационный микросервис на Go, который:
- Получает данные о заказах из Kafka
- Сохраняет их в PostgreSQL
- Кэширует в памяти для быстрого доступа
- Предоставляет HTTP API для получения заказов
- Имеет веб-интерфейс для просмотра заказов

## Структура проекта

```
OrderService/
├── cmd/
│   └── server/           # Основной сервер
├── internal/
│   ├── config/           # Конфигурация
│   ├── models/           # Модели данных
│   ├── database/         # Работа с БД
│   ├── cache/            # Кэш в памяти
│   ├── kafka/            # Kafka consumer
│   └── handlers/         # HTTP handlers
├── web/
│   └── templates/        # HTML шаблон
└── docker-compose.yml    # Docker окружение
```

## Установка и запуск

### 1. Установка зависимостей

```bash
# Убедитесь, что у вас установлен Go 1.21+
go version

# Скачайте зависимости
go mod download
go mod tidy
```

### 2. Запуск инфраструктуры

```bash
docker-compose up -d
```

Это запустит:
- PostgreSQL на порту 5432
- Kafka на порту 9092
- Kafka UI на http://localhost:8080

### 3. Запуск сервиса

```bash
go run cmd/server/main.go
```

Сервис будет доступен на http://localhost:8081

### 4. Проверка работы

```bash
# Откройте веб-интерфейс в браузере
http://localhost:8081

# Или проверьте API напрямую
# Windows PowerShell:
Invoke-WebRequest -Uri "http://localhost:8081/health"

# Linux/Mac/WSL:
curl http://localhost:8081/health
```

Введите ID заказа в веб-интерфейсе: `b563feb7b2b84b6test`

## API Endpoints

### GET /order/{orderUID}
Получить заказ по ID

**Пример запроса:**
```bash
# Linux/Mac/WSL
curl http://localhost:8081/order/b563feb7b2b84b6test

# Windows PowerShell
Invoke-WebRequest -Uri "http://localhost:8081/order/b563feb7b2b84b6test"

# Windows Command Prompt  
curl.exe http://localhost:8081/order/b563feb7b2b84b6test
```

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
  "customer_id": "test",
  "delivery_service": "meest",
  "shardkey": "9",
  "sm_id": 99,
  "date_created": "2021-11-26T06:22:19Z",
  "oof_shard": "1"
}
```

### GET /health
Проверка состояния сервиса

**Пример запроса:**
```bash
# Linux/Mac/WSL
curl http://localhost:8081/health

# Windows PowerShell
Invoke-WebRequest -Uri "http://localhost:8081/health"
```

**Пример ответа:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T12:00:00Z",
  "cache_size": 1
}
```

### GET /stats
Статистика кэша

**Пример запроса:**
```bash
# Linux/Mac/WSL
curl http://localhost:8081/stats

# Windows PowerShell
Invoke-WebRequest -Uri "http://localhost:8081/stats"
```

**Пример ответа:**
```json
{
  "cache_size": 1,
  "timestamp": "2024-01-01T12:00:00Z"
}
```

## Конфигурация

Сервис использует переменные окружения для конфигурации:

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `SERVER_PORT` | 8081 | Порт HTTP сервера |
| `SERVER_HOST` | localhost | Хост HTTP сервера |
| `DB_HOST` | localhost | Хост PostgreSQL |
| `DB_PORT` | 5432 | Порт PostgreSQL |
| `DB_USER` | orderservice | Пользователь БД |
| `DB_PASSWORD` | password | Пароль БД |
| `DB_NAME` | orderservice | Имя БД |
| `DB_SSLMODE` | disable | SSL режим БД |
| `KAFKA_BROKERS` | localhost:9092 | Адреса Kafka брокеров |
| `KAFKA_TOPIC` | orders | Топик Kafka |
| `KAFKA_GROUP_ID` | order-service | ID группы Kafka |
| `CACHE_MAX_SIZE` | 1000 | Максимальный размер кэша |


## Очистка

```bash
# Очистка артефактов сборки
rm -rf bin/
rm -f coverage.out coverage.html

# Остановка Docker сервисов
docker-compose down
```

## Примеры использования API

### Получение заказа по ID
```bash
# Linux/Mac/WSL
curl http://localhost:8081/order/b563feb7b2b84b6test

# Windows PowerShell
Invoke-WebRequest -Uri "http://localhost:8081/order/b563feb7b2b84b6test"

# Windows Command Prompt
curl.exe http://localhost:8081/order/b563feb7b2b84b6test
```

### Отправка заказа через API (если поддерживается)
```bash
# Linux/Mac/WSL
curl -X POST http://localhost:8081/order \
  -H "Content-Type: application/json" \
  -d @examples/order1_simple.json

# Windows PowerShell  
$headers = @{ "Content-Type" = "application/json" }
$body = Get-Content "examples/order1_simple.json" -Raw
Invoke-WebRequest -Uri "http://localhost:8081/order" -Method POST -Headers $headers -Body $body

# Windows Command Prompt
curl.exe -X POST http://localhost:8081/order ^
  -H "Content-Type: application/json" ^
  -d @examples/order1_simple.json
```

### Проверка состояния сервиса
```bash
# Health check
curl http://localhost:8081/health

# Статистика кэша  
curl http://localhost:8081/stats

# Windows PowerShell
Invoke-WebRequest -Uri "http://localhost:8081/health"
Invoke-WebRequest -Uri "http://localhost:8081/stats"
```

### Примеры тестовых данных
В каталоге `examples/` находятся готовые JSON файлы:
- `order1_simple.json` - простой заказ с одним товаром
- `order2_multiple_items.json` - заказ с несколькими товарами
- `order3_with_discount.json` - заказ со скидками
- `order4_international.json` - международный заказ (EUR)
- `order5_invalid.json` - некорректный заказ для тестирования

## Структура базы данных

### Таблица orders

```sql
CREATE TABLE orders (
    order_uid VARCHAR(255) PRIMARY KEY,
    track_number VARCHAR(255),
    entry VARCHAR(255),
    delivery JSONB,
    payment JSONB,
    items JSONB,
    locale VARCHAR(10),
    internal_signature VARCHAR(255),
    customer_id VARCHAR(255),
    delivery_service VARCHAR(255),
    shard_key VARCHAR(255),
    sm_id INTEGER,
    date_created TIMESTAMP,
    oof_shard VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Kafka

### Топик orders

Сервис подписывается на топик `orders` и обрабатывает сообщения в формате JSON.

**Пример сообщения:**
```json
{
  "order_uid": "b563feb7b2b84b6test",
  "track_number": "WBILMTESTTRACK",
  "entry": "WBIL",
  "delivery": { ... },
  "payment": { ... },
  "items": [ ... ],
  "locale": "en",
  "customer_id": "test",
  "delivery_service": "meest",
  "shardkey": "9",
  "sm_id": 99,
  "date_created": "2021-11-26T06:22:19Z",
  "oof_shard": "1"
}
```

## Мониторинг

- Health check endpoint: `/health`
- Статистика кэша: `/stats`
