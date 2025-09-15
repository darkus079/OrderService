package kafka

import (
	"encoding/json"
	"fmt"
	"orderservice/internal/config"
	"orderservice/internal/models"
	"strings"
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

// TestConsumer_parseMessageEdgeCases tests edge cases for message parsing
func TestConsumer_parseMessageEdgeCases(t *testing.T) {
	consumer := NewConsumer(&config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "test-group",
	})

	tests := []struct {
		name        string
		messageData []byte
		expectError bool
		description string
	}{
		{
			name:        "Null bytes in JSON",
			messageData: []byte(`{"order_uid": "test\u0000order", "track_number": "TRACK123"}`),
			expectError: false,
			description: "Should handle null bytes in JSON",
		},
		{
			name:        "Very long order UID",
			messageData: []byte(fmt.Sprintf(`{"order_uid": "%s", "track_number": "TRACK123"}`, strings.Repeat("a", 1000))),
			expectError: false,
			description: "Should handle very long order UIDs",
		},
		{
			name:        "Unicode characters",
			messageData: []byte(`{"order_uid": "заказ-тест-123-🛍️", "track_number": "TRACK123"}`),
			expectError: false,
			description: "Should handle Unicode characters",
		},
		{
			name:        "Empty string order UID",
			messageData: []byte(`{"order_uid": "", "track_number": "TRACK123"}`),
			expectError: true,
			description: "Should reject empty order UID",
		},
		{
			name:        "Empty string track number",
			messageData: []byte(`{"order_uid": "test-order-1", "track_number": ""}`),
			expectError: true,
			description: "Should reject empty track number",
		},
		{
			name:        "Null order UID",
			messageData: []byte(`{"order_uid": null, "track_number": "TRACK123"}`),
			expectError: true,
			description: "Should reject null order UID",
		},
		{
			name:        "Missing quotes in JSON",
			messageData: []byte(`{order_uid: "test-order-1", track_number: "TRACK123"}`),
			expectError: true,
			description: "Should reject invalid JSON syntax",
		},
		{
			name:        "Trailing comma in JSON",
			messageData: []byte(`{"order_uid": "test-order-1", "track_number": "TRACK123",}`),
			expectError: true,
			description: "Should reject JSON with trailing comma",
		},
		{
			name:        "Very nested JSON",
			messageData: []byte(`{"order_uid": "test-order-1", "track_number": "TRACK123", "nested": {"level1": {"level2": {"level3": {"data": "value"}}}}}`),
			expectError: false,
			description: "Should handle deeply nested JSON",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			order, err := consumer.parseMessage(tt.messageData)

			if tt.expectError {
				if err == nil {
					t.Errorf("%s: Expected error but got none", tt.description)
				}
				return
			}

			if err != nil {
				t.Errorf("%s: Unexpected error: %v", tt.description, err)
				return
			}

			if order == nil {
				t.Errorf("%s: Expected order but got nil", tt.description)
				return
			}

			t.Logf("%s: Successfully parsed order with UID: %s", tt.description, order.OrderUID)
		})
	}
}

// TestConsumer_parseMessageLargeData tests parsing of large JSON messages
func TestConsumer_parseMessageLargeData(t *testing.T) {
	consumer := NewConsumer(&config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "test-group",
	})

	// Create order with large data
	largeOrder := models.Order{
		OrderUID:          "large-order-test-001",
		TrackNumber:       "LARGE_TRACK_001",
		Entry:             "WBIL",
		Locale:            "en",
		InternalSignature: strings.Repeat("signature", 100), // Large signature
		CustomerID:        "large_customer",
		DeliveryService:   "meest",
		ShardKey:          "1",
		SMID:              1,
		DateCreated:       time.Now(),
		OOFShard:          "1",
		Delivery: models.Delivery{
			Name:    "Large Order Customer",
			Phone:   "+380501234567",
			Zip:     "01001",
			City:    "Kyiv",
			Address: strings.Repeat("Very long address ", 50), // Large address
			Region:  "Kyivska",
			Email:   "large@test.com",
		},
		Payment: models.Payment{
			Transaction:  "large_txn_001",
			Currency:     "USD",
			Provider:     "wbpay",
			Amount:       50000,
			PaymentDT:    time.Now().Unix(),
			Bank:         "alpha",
			DeliveryCost: 500,
			GoodsTotal:   49500,
			CustomFee:    0,
		},
	}

	// Add 100 items to create very large JSON
	largeOrder.Items = make([]models.Item, 100)
	for i := 0; i < 100; i++ {
		largeOrder.Items[i] = models.Item{
			ChrtID:      1000 + i,
			TrackNumber: largeOrder.TrackNumber,
			Price:       (i%10 + 1) * 100,
			RID:         fmt.Sprintf("large_rid_%03d", i+1),
			Name:        fmt.Sprintf("Large Item %d with very long name %s", i+1, strings.Repeat("description ", 10)),
			Sale:        i % 50,
			Size:        fmt.Sprintf("Size_%d", i%10),
			TotalPrice:  ((i%10 + 1) * 100) * (100 - i%50) / 100,
			NMID:        2000 + i,
			Brand:       fmt.Sprintf("Large Brand %d", (i%20)+1),
			Status:      202,
		}
	}

	messageData, err := json.Marshal(largeOrder)
	if err != nil {
		t.Fatalf("Failed to marshal large order: %v", err)
	}

	t.Logf("Large JSON size: %d bytes", len(messageData))

	start := time.Now()
	parsedOrder, err := consumer.parseMessage(messageData)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Failed to parse large message: %v", err)
	}

	t.Logf("Large JSON parsing time: %v", duration)

	if parsedOrder.OrderUID != largeOrder.OrderUID {
		t.Errorf("Expected OrderUID %s, got %s", largeOrder.OrderUID, parsedOrder.OrderUID)
	}

	if len(parsedOrder.Items) != len(largeOrder.Items) {
		t.Errorf("Expected %d items, got %d", len(largeOrder.Items), len(parsedOrder.Items))
	}

	// Verify a few items
	for i := 0; i < min(5, len(parsedOrder.Items)); i++ {
		if parsedOrder.Items[i].ChrtID != largeOrder.Items[i].ChrtID {
			t.Errorf("Item %d: Expected ChrtID %d, got %d", i, largeOrder.Items[i].ChrtID, parsedOrder.Items[i].ChrtID)
		}
	}
}

// TestConsumer_parseMessageMalformedData tests handling of malformed data
func TestConsumer_parseMessageMalformedData(t *testing.T) {
	consumer := NewConsumer(&config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "test-group",
	})

	tests := []struct {
		name        string
		messageData []byte
		description string
	}{
		{
			name:        "Binary data",
			messageData: []byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD},
			description: "Should handle binary data gracefully",
		},
		{
			name:        "Random bytes",
			messageData: []byte("random non-json data !@#$%^&*()"),
			description: "Should handle random text data",
		},
		{
			name:        "Incomplete JSON opening",
			messageData: []byte(`{"order_uid": "test`),
			description: "Should handle incomplete JSON",
		},
		{
			name:        "Incomplete JSON closing",
			messageData: []byte(`{"order_uid": "test", "track_number": "TRACK123"`),
			description: "Should handle JSON without closing brace",
		},
		{
			name:        "Double encoded JSON",
			messageData: []byte(`"{\"order_uid\": \"test\", \"track_number\": \"TRACK123\"}"`),
			description: "Should handle double-encoded JSON",
		},
		{
			name:        "Very large invalid JSON",
			messageData: []byte(strings.Repeat("invalid", 10000)),
			description: "Should handle very large invalid data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			order, err := consumer.parseMessage(tt.messageData)

			// All these cases should result in errors
			if err == nil {
				t.Errorf("%s: Expected error but got none", tt.description)
			}

			if order != nil {
				t.Errorf("%s: Expected nil order but got: %+v", tt.description, order)
			}

			t.Logf("%s: Correctly handled with error: %v", tt.description, err)
		})
	}
}

// TestConsumer_NewConsumer tests consumer creation
func TestConsumer_NewConsumer(t *testing.T) {
	tests := []struct {
		name   string
		config *config.KafkaConfig
	}{
		{
			name: "Valid config",
			config: &config.KafkaConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "orders",
				GroupID: "test-group",
			},
		},
		{
			name: "Multiple brokers",
			config: &config.KafkaConfig{
				Brokers: []string{"broker1:9092", "broker2:9092", "broker3:9092"},
				Topic:   "orders",
				GroupID: "test-group-multi",
			},
		},
		{
			name: "Different topic",
			config: &config.KafkaConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "test-orders",
				GroupID: "test-group-custom",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			consumer := NewConsumer(tt.config)

			if consumer == nil {
				t.Error("Expected consumer to be created")
				return
			}

			if consumer.config.Topic != tt.config.Topic {
				t.Errorf("Expected topic %s, got %s", tt.config.Topic, consumer.config.Topic)
			}

			if consumer.config.GroupID != tt.config.GroupID {
				t.Errorf("Expected GroupID %s, got %s", tt.config.GroupID, consumer.config.GroupID)
			}

			if len(consumer.config.Brokers) != len(tt.config.Brokers) {
				t.Errorf("Expected %d brokers, got %d", len(tt.config.Brokers), len(consumer.config.Brokers))
			}

			// Test channels
			if consumer.OrderChannel() == nil {
				t.Error("Expected OrderChannel to be available")
			}

			if consumer.ErrorChannel() == nil {
				t.Error("Expected ErrorChannel to be available")
			}

			// Clean up
			consumer.Stop()
		})
	}
}

// TestConsumer_Channels tests consumer channels functionality
func TestConsumer_Channels(t *testing.T) {
	consumer := NewConsumer(&config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "test-group",
	})
	defer consumer.Stop()

	// Test OrderChannel
	orderCh := consumer.OrderChannel()
	if orderCh == nil {
		t.Fatal("Expected OrderChannel to be available")
	}

	// Test ErrorChannel
	errorCh := consumer.ErrorChannel()
	if errorCh == nil {
		t.Fatal("Expected ErrorChannel to be available")
	}

	// Test that channels are properly typed
	select {
	case <-orderCh:
		// This shouldn't block or panic
	case <-errorCh:
		// This shouldn't block or panic
	case <-time.After(10 * time.Millisecond):
		// Timeout is expected since no messages
	}
}

// TestConsumer_StopConsumer tests consumer stopping
func TestConsumer_StopConsumer(t *testing.T) {
	consumer := NewConsumer(&config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "test-group",
	})

	// Start consumer
	consumer.Start()

	// Let it run briefly
	time.Sleep(10 * time.Millisecond)

	// Stop consumer
	consumer.Stop()

	// Try to get from channels after stopping
	select {
	case order := <-consumer.OrderChannel():
		if order != nil {
			t.Error("Expected nil order from closed channel")
		}
	case err := <-consumer.ErrorChannel():
		if err != nil {
			t.Error("Expected nil error from closed channel")
		}
	case <-time.After(100 * time.Millisecond):
		t.Log("Channels properly closed or empty")
	}
}

// TestConsumer_parseMessageWithMissingFields tests parsing with various missing fields
func TestConsumer_parseMessageWithMissingFields(t *testing.T) {
	consumer := NewConsumer(&config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "test-group",
	})

	tests := []struct {
		name        string
		messageData string
		expectError bool
		description string
	}{
		{
			name:        "Missing delivery",
			messageData: `{"order_uid": "test-order-1", "track_number": "TRACK123"}`,
			expectError: false,
			description: "Should accept message without delivery",
		},
		{
			name:        "Missing payment",
			messageData: `{"order_uid": "test-order-1", "track_number": "TRACK123"}`,
			expectError: false,
			description: "Should accept message without payment",
		},
		{
			name:        "Missing items",
			messageData: `{"order_uid": "test-order-1", "track_number": "TRACK123"}`,
			expectError: false,
			description: "Should accept message without items",
		},
		{
			name:        "Only required fields",
			messageData: `{"order_uid": "test-order-1", "track_number": "TRACK123"}`,
			expectError: false,
			description: "Should accept message with only required fields",
		},
		{
			name:        "Empty objects",
			messageData: `{"order_uid": "test-order-1", "track_number": "TRACK123", "delivery": {}, "payment": {}, "items": []}`,
			expectError: false,
			description: "Should accept message with empty objects",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			order, err := consumer.parseMessage([]byte(tt.messageData))

			if tt.expectError {
				if err == nil {
					t.Errorf("%s: Expected error but got none", tt.description)
				}
				return
			}

			if err != nil {
				t.Errorf("%s: Unexpected error: %v", tt.description, err)
				return
			}

			if order == nil {
				t.Errorf("%s: Expected order but got nil", tt.description)
				return
			}

			if order.OrderUID != "test-order-1" {
				t.Errorf("%s: Expected OrderUID 'test-order-1', got %s", tt.description, order.OrderUID)
			}

			if order.TrackNumber != "TRACK123" {
				t.Errorf("%s: Expected TrackNumber 'TRACK123', got %s", tt.description, order.TrackNumber)
			}

			t.Logf("%s: Successfully parsed minimal order", tt.description)
		})
	}
}

// min helper function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// BenchmarkConsumer_parseMessage benchmarks message parsing
func BenchmarkConsumer_parseMessage(b *testing.B) {
	consumer := NewConsumer(&config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "test-group",
	})

	messageData := []byte(`{
		"order_uid": "benchmark-order-001",
		"track_number": "BENCH_TRACK_001",
		"entry": "WBIL",
		"delivery": {
			"name": "Benchmark Customer",
			"phone": "+380501234567",
			"zip": "01001",
			"city": "Kyiv",
			"address": "Benchmark Street 1",
			"region": "Kyivska",
			"email": "benchmark@test.com"
		},
		"payment": {
			"transaction": "bench_txn_001",
			"currency": "USD",
			"provider": "wbpay",
			"amount": 1000,
			"payment_dt": 1640995200,
			"bank": "alpha",
			"delivery_cost": 200,
			"goods_total": 800,
			"custom_fee": 0
		},
		"items": [
			{
				"chrt_id": 123456,
				"track_number": "BENCH_TRACK_001",
				"price": 400,
				"rid": "bench_rid_001",
				"name": "Benchmark Item",
				"sale": 10,
				"size": "M",
				"total_price": 360,
				"nm_id": 654321,
				"brand": "Benchmark Brand",
				"status": 202
			}
		],
		"locale": "en",
		"customer_id": "benchmark_customer",
		"delivery_service": "meest",
		"shardkey": "1",
		"sm_id": 1,
		"date_created": "2024-01-01T10:00:00Z",
		"oof_shard": "1"
	}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := consumer.parseMessage(messageData)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkConsumer_parseMessageLarge benchmarks parsing of large messages
func BenchmarkConsumer_parseMessageLarge(b *testing.B) {
	consumer := NewConsumer(&config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "test-group",
	})

	// Create large order
	largeOrder := models.Order{
		OrderUID:    "large-benchmark-order",
		TrackNumber: "LARGE_BENCH_TRACK",
		Entry:       "WBIL",
		Delivery: models.Delivery{
			Name:    "Large Benchmark Customer",
			Address: strings.Repeat("Long address ", 100),
		},
		Payment: models.Payment{
			Transaction: "large_bench_txn",
			Currency:    "USD",
			Amount:      10000,
		},
	}

	// Add 50 items
	largeOrder.Items = make([]models.Item, 50)
	for i := 0; i < 50; i++ {
		largeOrder.Items[i] = models.Item{
			ChrtID: 1000 + i,
			Name:   fmt.Sprintf("Large Benchmark Item %d %s", i+1, strings.Repeat("description ", 20)),
			Price:  (i + 1) * 100,
		}
	}

	messageData, err := json.Marshal(largeOrder)
	if err != nil {
		b.Fatal(err)
	}

	b.Logf("Large message size: %d bytes", len(messageData))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := consumer.parseMessage(messageData)
		if err != nil {
			b.Fatal(err)
		}
	}
}
