package kafka

import (
	"encoding/json"
	"orderservice/internal/config"
	"orderservice/internal/models"
	"testing"
	"time"
)

func TestConsumer_parseMessage(t *testing.T) {
	consumer := NewConsumer(&config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "test-group",
	})

	tests := []struct {
		name        string
		messageData []byte
		expectError bool
		expectedUID string
	}{
		{
			name: "valid message",
			messageData: []byte(`{
				"order_uid": "test-order-1",
				"track_number": "TRACK123",
				"entry": "WBIL",
				"delivery": {
					"name": "Test User",
					"phone": "+1234567890",
					"zip": "12345",
					"city": "Test City",
					"address": "Test Address",
					"region": "Test Region",
					"email": "test@example.com"
				},
				"payment": {
					"transaction": "test-transaction",
					"request_id": "",
					"currency": "USD",
					"provider": "test-provider",
					"amount": 1000,
					"payment_dt": 1637907727,
					"bank": "test-bank",
					"delivery_cost": 100,
					"goods_total": 900,
					"custom_fee": 0
				},
				"items": [
					{
						"chrt_id": 123,
						"track_number": "TRACK123",
						"price": 500,
						"rid": "test-rid",
						"name": "Test Item",
						"sale": 10,
						"size": "M",
						"total_price": 450,
						"nm_id": 456,
						"brand": "Test Brand",
						"status": 202
					}
				],
				"locale": "en",
				"internal_signature": "",
				"customer_id": "customer1",
				"delivery_service": "test-service",
				"shardkey": "1",
				"sm_id": 99,
				"date_created": "2021-11-26T06:22:19Z",
				"oof_shard": "1"
			}`),
			expectError: false,
			expectedUID: "test-order-1",
		},
		{
			name:        "missing order_uid",
			messageData: []byte(`{"track_number": "TRACK123"}`),
			expectError: true,
		},
		{
			name:        "missing track_number",
			messageData: []byte(`{"order_uid": "test-order-1"}`),
			expectError: true,
		},
		{
			name:        "invalid JSON",
			messageData: []byte(`invalid json`),
			expectError: true,
		},
		{
			name:        "empty message",
			messageData: []byte(``),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			order, err := consumer.parseMessage(tt.messageData)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if order == nil {
				t.Errorf("Expected order but got nil")
				return
			}

			if order.OrderUID != tt.expectedUID {
				t.Errorf("Expected OrderUID %s, got %s", tt.expectedUID, order.OrderUID)
			}

			if order.DateCreated.IsZero() {
				t.Error("Expected DateCreated to be set")
			}
		})
	}
}

func TestConsumer_parseMessageWithDateCreated(t *testing.T) {
	consumer := NewConsumer(&config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "test-group",
	})

	messageData := []byte(`{
		"order_uid": "test-order-1",
		"track_number": "TRACK123",
		"date_created": "2021-11-26T06:22:19Z"
	}`)

	order, err := consumer.parseMessage(messageData)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedTime, _ := time.Parse(time.RFC3339, "2021-11-26T06:22:19Z")
	if !order.DateCreated.Equal(expectedTime) {
		t.Errorf("Expected DateCreated %v, got %v", expectedTime, order.DateCreated)
	}
}

func TestConsumer_parseMessageWithoutDateCreated(t *testing.T) {
	consumer := NewConsumer(&config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "test-group",
	})

	messageData := []byte(`{
		"order_uid": "test-order-1",
		"track_number": "TRACK123"
	}`)

	before := time.Now()
	order, err := consumer.parseMessage(messageData)
	after := time.Now()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if order.DateCreated.Before(before) || order.DateCreated.After(after) {
		t.Errorf("Expected DateCreated to be between %v and %v, got %v", before, after, order.DateCreated)
	}
}

func TestConsumer_parseMessageComplexOrder(t *testing.T) {
	consumer := NewConsumer(&config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "test-group",
	})

	complexOrder := models.Order{
		OrderUID:    "b563feb7b2b84b6test",
		TrackNumber: "WBILMTESTTRACK",
		Entry:       "WBIL",
		Delivery: models.Delivery{
			Name:    "Test Testov",
			Phone:   "+9720000000",
			Zip:     "2639809",
			City:    "Kiryat Mozkin",
			Address: "Ploshad Mira 15",
			Region:  "Kraiot",
			Email:   "test@gmail.com",
		},
		Payment: models.Payment{
			Transaction:  "b563feb7b2b84b6test",
			RequestID:    "",
			Currency:     "USD",
			Provider:     "wbpay",
			Amount:       1817,
			PaymentDT:    1637907727,
			Bank:         "alpha",
			DeliveryCost: 1500,
			GoodsTotal:   317,
			CustomFee:    0,
		},
		Items: []models.Item{
			{
				ChrtID:      9934930,
				TrackNumber: "WBILMTESTTRACK",
				Price:       453,
				RID:         "ab4219087a764ae0btest",
				Name:        "Mascaras",
				Sale:        30,
				Size:        "0",
				TotalPrice:  317,
				NMID:        2389212,
				Brand:       "Vivienne Sabo",
				Status:      202,
			},
		},
		Locale:            "en",
		InternalSignature: "",
		CustomerID:        "test",
		DeliveryService:   "meest",
		ShardKey:          "9",
		SMID:              99,
		DateCreated:       time.Date(2021, 11, 26, 6, 22, 19, 0, time.UTC),
		OOFShard:          "1",
	}

	messageData, err := json.Marshal(complexOrder)
	if err != nil {
		t.Fatalf("Failed to marshal order: %v", err)
	}

	parsedOrder, err := consumer.parseMessage(messageData)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if parsedOrder.OrderUID != complexOrder.OrderUID {
		t.Errorf("Expected OrderUID %s, got %s", complexOrder.OrderUID, parsedOrder.OrderUID)
	}

	if parsedOrder.Delivery.Name != complexOrder.Delivery.Name {
		t.Errorf("Expected Delivery.Name %s, got %s", complexOrder.Delivery.Name, parsedOrder.Delivery.Name)
	}

	if parsedOrder.Payment.Amount != complexOrder.Payment.Amount {
		t.Errorf("Expected Payment.Amount %d, got %d", complexOrder.Payment.Amount, parsedOrder.Payment.Amount)
	}

	if len(parsedOrder.Items) != len(complexOrder.Items) {
		t.Errorf("Expected %d items, got %d", len(complexOrder.Items), len(parsedOrder.Items))
	}

	if len(parsedOrder.Items) > 0 && parsedOrder.Items[0].Name != complexOrder.Items[0].Name {
		t.Errorf("Expected first item name %s, got %s", complexOrder.Items[0].Name, parsedOrder.Items[0].Name)
	}
}
