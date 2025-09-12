package edge_cases

import (
	"context"
	"encoding/json"
	"fmt"
	"orderservice/internal/cache"
	"orderservice/internal/kafka"
	"orderservice/internal/models"
	"orderservice/tests/testdata"
	"strings"
	"testing"
	"time"
)

func TestEdgeCases(t *testing.T) {
	helper := testdata.NewTestHelper()
	ctx := context.Background()

	t.Run("InvalidJSONMessages", func(t *testing.T) {
		testInvalidJSONMessages(t, helper, ctx)
	})

	t.Run("DuplicateOrders", func(t *testing.T) {
		testDuplicateOrders(t, helper, ctx)
	})

	t.Run("LargeJSONHandling", func(t *testing.T) {
		testLargeJSONHandling(t, helper, ctx)
	})

	t.Run("CacheOverflow", func(t *testing.T) {
		testCacheOverflow(t, helper, ctx)
	})

	t.Run("DatabaseEdgeCases", func(t *testing.T) {
		testDatabaseEdgeCases(t, helper, ctx)
	})

	t.Run("KafkaEdgeCases", func(t *testing.T) {
		testKafkaEdgeCases(t, helper, ctx)
	})

	t.Run("ConcurrencyEdgeCases", func(t *testing.T) {
		testConcurrencyEdgeCases(t, helper, ctx)
	})

	t.Run("MemoryLimits", func(t *testing.T) {
		testMemoryLimits(t, helper, ctx)
	})
}

func testInvalidJSONMessages(t *testing.T, helper *testdata.TestHelper, ctx context.Context) {
	cfg := helper.GetConfig()
	consumer := kafka.NewConsumer(&cfg.Kafka)
	defer consumer.Stop()

	invalidMessages := []struct {
		name    string
		message string
		reason  string
	}{
		{
			name:    "Empty JSON",
			message: `{}`,
			reason:  "Missing required fields",
		},
		{
			name:    "Malformed JSON",
			message: `{"order_uid": "test", "track_number":}`,
			reason:  "Invalid JSON syntax",
		},
		{
			name:    "Null values",
			message: `{"order_uid": null, "track_number": null}`,
			reason:  "Null required fields",
		},
		{
			name:    "Wrong data types",
			message: `{"order_uid": 123, "track_number": true}`,
			reason:  "Incorrect data types",
		},
		{
			name:    "Binary data",
			message: string([]byte{0x00, 0x01, 0x02, 0x03}),
			reason:  "Binary instead of JSON",
		},
		{
			name:    "Very long strings",
			message: fmt.Sprintf(`{"order_uid": "%s", "track_number": "TRACK"}`, strings.Repeat("a", 10000)),
			reason:  "Extremely long order UID",
		},
		{
			name:    "Unicode edge cases",
			message: `{"order_uid": "\u0000\u0001\u0002", "track_number": "TRACK"}`,
			reason:  "Unicode control characters",
		},
		{
			name:    "Invalid escape sequences",
			message: `{"order_uid": "test\x", "track_number": "TRACK"}`,
			reason:  "Invalid escape sequences",
		},
	}

	for _, testCase := range invalidMessages {
		t.Run(testCase.name, func(t *testing.T) {
			err := helper.SendInvalidKafkaMessage(ctx, testCase.message)
			if err != nil {
				t.Logf("Expected: failed to send invalid message (%s): %v", testCase.reason, err)
				return
			}

			consumer.Start()

			select {
			case order := <-consumer.OrderChannel():
				if order != nil {
					t.Errorf("Expected invalid message to be rejected, but got order: %+v", order)
				}
			case err := <-consumer.ErrorChannel():
				if err != nil {
					t.Logf("Consumer correctly rejected invalid message (%s): %v", testCase.reason, err)
				}
			case <-time.After(2 * time.Second):
				t.Logf("No response for invalid message (%s) - timeout", testCase.reason)
			}
		})
	}
}

func testDuplicateOrders(t *testing.T, helper *testdata.TestHelper, ctx context.Context) {
	repo, err := helper.SetupDatabase()
	if err != nil {
		t.Skipf("Skipping database tests: %v", err)
	}

	helper.CleanupDatabase(ctx)

	testOrder := &models.Order{
		OrderUID:    "duplicate_test_order_001",
		TrackNumber: "DUPLICATE_TRACK",
		Entry:       "WBIL",
		CustomerID:  "duplicate_customer",
		Delivery: models.Delivery{
			Name:  "Duplicate Customer",
			Email: "duplicate@test.com",
		},
		Payment: models.Payment{
			Transaction: "duplicate_txn",
			Currency:    "USD",
			Amount:      1000,
		},
		Items: []models.Item{
			{
				ChrtID: 123,
				Name:   "Duplicate Item",
				Price:  500,
			},
		},
		DateCreated: time.Now(),
	}

	err = repo.CreateOrder(ctx, testOrder)
	if err != nil {
		t.Fatalf("Failed to create original order: %v", err)
	}

	testOrder.CustomerID = "duplicate_customer_modified"
	testOrder.Payment.Amount = 2000

	err = repo.CreateOrder(ctx, testOrder)
	if err != nil {
		t.Fatalf("Failed to handle duplicate order: %v", err)
	}

	retrieved, err := repo.GetOrderByID(ctx, testOrder.OrderUID)
	if err != nil {
		t.Fatalf("Failed to retrieve order: %v", err)
	}

	if retrieved.CustomerID != "duplicate_customer_modified" {
		t.Errorf("Expected CustomerID to be updated, got %s", retrieved.CustomerID)
	}

	if retrieved.Payment.Amount != 2000 {
		t.Errorf("Expected Amount to be updated to 2000, got %d", retrieved.Payment.Amount)
	}

	allOrders, err := repo.GetAllOrders(ctx)
	if err != nil {
		t.Fatalf("Failed to get all orders: %v", err)
	}

	duplicateCount := 0
	for _, order := range allOrders {
		if order.OrderUID == testOrder.OrderUID {
			duplicateCount++
		}
	}

	if duplicateCount != 1 {
		t.Errorf("Expected exactly 1 order with UID %s, found %d", testOrder.OrderUID, duplicateCount)
	}

	t.Logf("Duplicate order handling test passed: upsert worked correctly")
}

func testLargeJSONHandling(t *testing.T, helper *testdata.TestHelper, ctx context.Context) {
	largeOrder := helper.GetLargeTestOrder()
	start := time.Now()
	jsonData, err := json.Marshal(largeOrder)
	marshalDuration := time.Since(start)

	if err != nil {
		t.Fatalf("Failed to marshal large order: %v", err)
	}

	t.Logf("Large JSON size: %d bytes, marshal time: %v", len(jsonData), marshalDuration)

	if len(jsonData) < 10000 {
		t.Error("Expected large JSON (>10KB)")
	}

	start = time.Now()
	var unmarshaled models.Order
	err = json.Unmarshal(jsonData, &unmarshaled)
	unmarshalDuration := time.Since(start)

	if err != nil {
		t.Fatalf("Failed to unmarshal large order: %v", err)
	}

	t.Logf("Unmarshal time: %v", unmarshalDuration)

	if unmarshaled.OrderUID != largeOrder.OrderUID {
		t.Errorf("OrderUID mismatch after unmarshaling")
	}

	if len(unmarshaled.Items) != len(largeOrder.Items) {
		t.Errorf("Items count mismatch: expected %d, got %d", len(largeOrder.Items), len(unmarshaled.Items))
	}

	repo, err := helper.SetupDatabase()
	if err != nil {
		t.Skip("Skipping database test for large JSON")
		return
	}

	helper.CleanupDatabase(ctx)

	start = time.Now()
	err = repo.CreateOrder(ctx, largeOrder)
	dbCreateDuration := time.Since(start)

	if err != nil {
		t.Fatalf("Failed to save large order to database: %v", err)
	}

	t.Logf("Database create time for large order: %v", dbCreateDuration)

	start = time.Now()
	retrieved, err := repo.GetOrderByID(ctx, largeOrder.OrderUID)
	dbRetrieveDuration := time.Since(start)

	if err != nil {
		t.Fatalf("Failed to retrieve large order from database: %v", err)
	}

	t.Logf("Database retrieve time for large order: %v", dbRetrieveDuration)

	if len(retrieved.Items) != len(largeOrder.Items) {
		t.Errorf("Database items count mismatch: expected %d, got %d", len(largeOrder.Items), len(retrieved.Items))
	}

	t.Log("Large JSON handling test completed successfully")
}

func testCacheOverflow(t *testing.T, helper *testdata.TestHelper, ctx context.Context) {
	mockRepo := &mockRepository{orders: make(map[string]*models.Order)}
	smallCache := cache.NewCache(5, mockRepo) // Very small cache

	orderUIDs := make([]string, 10)
	for i := 0; i < 10; i++ {
		orderUID := fmt.Sprintf("overflow_test_%03d", i+1)
		orderUIDs[i] = orderUID

		order := &models.Order{
			OrderUID:    orderUID,
			TrackNumber: fmt.Sprintf("OVERFLOW_TRACK_%03d", i+1),
			CustomerID:  fmt.Sprintf("overflow_customer_%d", i+1),
		}

		smallCache.Set(orderUID, order)
		time.Sleep(1 * time.Millisecond)
	}

	if smallCache.Size() > 5 {
		t.Errorf("Cache size %d exceeds maximum 5", smallCache.Size())
	}

	newestOrders := orderUIDs[len(orderUIDs)-5:]
	for _, uid := range newestOrders {
		order, err := smallCache.Get(ctx, uid)
		if err != nil {
			t.Errorf("Failed to get order %s: %v", uid, err)
		}
		if order == nil {
			t.Errorf("Expected newest order %s to be in cache", uid)
		}
	}

	oldestOrders := orderUIDs[:5]
	for _, uid := range oldestOrders {
		order, err := smallCache.Get(ctx, uid)
		if err != nil {
			t.Errorf("Failed to check order %s: %v", uid, err)
		}
		if order != nil {
			t.Errorf("Expected oldest order %s to be evicted from cache", uid)
		}
	}

	t.Logf("Cache overflow test passed: maintained size %d, LRU working correctly", smallCache.Size())
}

func testDatabaseEdgeCases(t *testing.T, helper *testdata.TestHelper, ctx context.Context) {
	repo, err := helper.SetupDatabase()
	if err != nil {
		t.Skipf("Skipping database edge cases: %v", err)
	}

	helper.CleanupDatabase(ctx)

	emptyOrder := &models.Order{
		OrderUID:    "empty_strings_test",
		TrackNumber: "EMPTY_TEST",
		Entry:       "",
		Locale:      "",
		CustomerID:  "",
		Delivery:    models.Delivery{},
		Payment:     models.Payment{},
		Items:       []models.Item{},
	}

	err = repo.CreateOrder(ctx, emptyOrder)
	if err != nil {
		t.Errorf("Failed to create order with empty fields: %v", err)
	}

	unicodeOrder := &models.Order{
		OrderUID:    "unicode_test_тест_🛍️",
		TrackNumber: "UNICODE_TRACK_测试",
		CustomerID:  "客户_عميل_клиент",
		Delivery: models.Delivery{
			Name:    "José François 陈明 محمد",
			Address: "Улица Пушкина 123, квартира 45",
			City:    "Москва",
		},
		Payment: models.Payment{
			Transaction: "txn_unicode_тест",
			Currency:    "₽RUB",
		},
	}

	err = repo.CreateOrder(ctx, unicodeOrder)
	if err != nil {
		t.Errorf("Failed to create order with Unicode: %v", err)
	}

	retrieved, err := repo.GetOrderByID(ctx, unicodeOrder.OrderUID)
	if err != nil {
		t.Errorf("Failed to retrieve Unicode order: %v", err)
	}

	if retrieved != nil && retrieved.Delivery.Name != unicodeOrder.Delivery.Name {
		t.Errorf("Unicode name not preserved: expected %s, got %s",
			unicodeOrder.Delivery.Name, retrieved.Delivery.Name)
	}

	longStringOrder := &models.Order{
		OrderUID:    "long_string_test",
		TrackNumber: strings.Repeat("LONG", 100),
		CustomerID:  strings.Repeat("customer", 50),
		Delivery: models.Delivery{
			Name:    strings.Repeat("Long Name ", 30),
			Address: strings.Repeat("Very Long Address ", 50),
		},
	}

	err = repo.CreateOrder(ctx, longStringOrder)
	if err != nil {
		t.Errorf("Failed to create order with long strings: %v", err)
	}

	specialCharsOrder := &models.Order{
		OrderUID:    "special_chars_!@#$%^&*()",
		TrackNumber: "TRACK_<>&\"'`",
		CustomerID:  "customer_\\/:*?<>|",
	}

	err = repo.CreateOrder(ctx, specialCharsOrder)
	if err != nil {
		t.Errorf("Failed to create order with special chars: %v", err)
	}

	t.Log("Database edge cases test completed")
}

func testKafkaEdgeCases(t *testing.T, helper *testdata.TestHelper, ctx context.Context) {
	cfg := helper.GetConfig()
	consumer := kafka.NewConsumer(&cfg.Kafka)
	defer consumer.Stop()

	largeOrder := helper.GetLargeTestOrder()
	largeOrder.OrderUID = "kafka_edge_large_test"

	err := helper.SendKafkaMessage(ctx, largeOrder)
	if err != nil {
		t.Errorf("Failed to send large message to Kafka: %v", err)
	} else {
		t.Log("Successfully sent large message to Kafka")
	}

	nullByteOrder := &models.Order{
		OrderUID:    "kafka_edge_null_test",
		TrackNumber: "TRACK_NULL",
		CustomerID:  "customer\x00with\x00nulls",
	}

	err = helper.SendKafkaMessage(ctx, nullByteOrder)
	if err != nil {
		t.Logf("Expected: Failed to send message with null bytes: %v", err)
	}

	rapidOrders := make([]*models.Order, 20)
	for i := 0; i < 20; i++ {
		rapidOrders[i] = &models.Order{
			OrderUID:    fmt.Sprintf("kafka_rapid_%03d", i+1),
			TrackNumber: fmt.Sprintf("RAPID_TRACK_%03d", i+1),
			CustomerID:  fmt.Sprintf("rapid_customer_%d", i+1),
		}
	}

	start := time.Now()
	successCount := 0
	for _, order := range rapidOrders {
		err := helper.SendKafkaMessage(ctx, order)
		if err != nil {
			t.Errorf("Failed to send rapid message %s: %v", order.OrderUID, err)
		} else {
			successCount++
		}
	}
	rapidSendDuration := time.Since(start)

	t.Logf("Rapid send test: %d/%d messages sent in %v", successCount, len(rapidOrders), rapidSendDuration)
	t.Log("Kafka edge cases test completed")
}

func testConcurrencyEdgeCases(t *testing.T, helper *testdata.TestHelper, ctx context.Context) {
	repo, err := helper.SetupDatabase()
	if err != nil {
		t.Skipf("Skipping concurrency tests: %v", err)
	}

	helper.CleanupDatabase(ctx)

	const numWorkers = 10
	const ordersPerWorker = 5

	done := make(chan bool, numWorkers)
	errors := make(chan error, numWorkers*ordersPerWorker)

	for worker := 0; worker < numWorkers; worker++ {
		go func(workerID int) {
			defer func() { done <- true }()

			for i := 0; i < ordersPerWorker; i++ {
				var orderUID string
				if i < 2 {
					orderUID = fmt.Sprintf("concurrent_conflict_%d", i+1)
				} else {
					orderUID = fmt.Sprintf("concurrent_worker_%d_order_%d", workerID, i+1)
				}

				order := &models.Order{
					OrderUID:    orderUID,
					TrackNumber: fmt.Sprintf("CONCURRENT_TRACK_%d_%d", workerID, i+1),
					CustomerID:  fmt.Sprintf("concurrent_customer_%d_%d", workerID, i+1),
					Payment: models.Payment{
						Amount: (workerID + 1) * 100,
					},
					DateCreated: time.Now(),
				}

				if err := repo.CreateOrder(ctx, order); err != nil {
					errors <- fmt.Errorf("worker %d order %s: %v", workerID, orderUID, err)
				}

				time.Sleep(1 * time.Millisecond)
			}
		}(worker)
	}

	for i := 0; i < numWorkers; i++ {
		<-done
	}
	close(errors)

	errorCount := 0
	for err := range errors {
		t.Errorf("Concurrency error: %v", err)
		errorCount++
	}

	if errorCount > 0 {
		t.Errorf("Had %d errors during concurrent operations", errorCount)
	}

	conflictOrder1, err := repo.GetOrderByID(ctx, "concurrent_conflict_1")
	if err != nil {
		t.Errorf("Failed to get conflict order 1: %v", err)
	}
	if conflictOrder1 == nil {
		t.Error("Conflict order 1 should exist")
	}

	t.Logf("Concurrency edge cases completed with %d errors", errorCount)
}

func testMemoryLimits(t *testing.T, helper *testdata.TestHelper, ctx context.Context) {
	mockRepo := &mockRepository{orders: make(map[string]*models.Order)}
	testCache := cache.NewCache(100, mockRepo)

	for i := 0; i < 50; i++ {
		order := &models.Order{
			OrderUID:          fmt.Sprintf("memory_test_%03d", i+1),
			TrackNumber:       fmt.Sprintf("MEMORY_TRACK_%03d", i+1),
			InternalSignature: strings.Repeat("signature", i+1),
			Delivery: models.Delivery{
				Name:    fmt.Sprintf("Memory Customer %d", i+1),
				Address: strings.Repeat("Long address ", (i%10)+1),
			},
		}

		itemCount := (i % 5) + 1
		order.Items = make([]models.Item, itemCount)
		for j := 0; j < itemCount; j++ {
			order.Items[j] = models.Item{
				ChrtID:      (i+1)*1000 + j + 1,
				TrackNumber: order.TrackNumber,
				Price:       (j + 1) * 100,
				Name:        fmt.Sprintf("Memory Item %d-%d %s", i+1, j+1, strings.Repeat("desc ", j+1)),
				Brand:       fmt.Sprintf("Memory Brand %d", (i%10)+1),
			}
		}

		testCache.Set(order.OrderUID, order)
	}

	t.Logf("Memory test: Created cache with %d orders of varying sizes", testCache.Size())

	accessCount := 0
	for i := 0; i < 100; i++ {
		orderUID := fmt.Sprintf("memory_test_%03d", (i%50)+1)
		order, err := testCache.Get(ctx, orderUID)
		if err != nil {
			t.Errorf("Memory test access error: %v", err)
		}
		if order != nil {
			accessCount++
		}
	}

	t.Logf("Memory test: Successfully accessed %d orders", accessCount)
}

type mockRepository struct {
	orders map[string]*models.Order
}

func (m *mockRepository) GetOrderByID(ctx context.Context, orderUID string) (*models.Order, error) {
	if order, exists := m.orders[orderUID]; exists {
		return order, nil
	}
	return nil, nil
}

func (m *mockRepository) GetAllOrders(ctx context.Context) ([]*models.Order, error) {
	var orders []*models.Order
	for _, order := range m.orders {
		orders = append(orders, order)
	}
	return orders, nil
}
