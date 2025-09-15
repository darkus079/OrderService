package database

import (
	"context"
	"database/sql"
	"fmt"
	"orderservice/internal/config"
	"orderservice/internal/models"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func TestPostgresRepository_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	cfg := &config.DatabaseConfig{
		Host:     "localhost",
		Port:     "5432",
		User:     "orderservice",
		Password: "password",
		DBName:   "orderservice",
		SSLMode:  "disable",
	}

	repo, err := NewPostgresRepository(cfg)
	if err != nil {
		t.Skipf("Skipping integration tests: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	cleanupTestData(t, repo, ctx)

	t.Run("CreateOrder", func(t *testing.T) {
		testCreateOrder(t, repo, ctx)
	})

	t.Run("GetOrderByID", func(t *testing.T) {
		testGetOrderByID(t, repo, ctx)
	})

	t.Run("GetAllOrders", func(t *testing.T) {
		testGetAllOrders(t, repo, ctx)
	})

	t.Run("UpdateOrder", func(t *testing.T) {
		testUpdateOrder(t, repo, ctx)
	})

	t.Run("DeleteOrder", func(t *testing.T) {
		testDeleteOrder(t, repo, ctx)
	})

	t.Run("ConcurrentOperations", func(t *testing.T) {
		testConcurrentOperations(t, repo, ctx)
	})

	t.Run("LargeOrders", func(t *testing.T) {
		testLargeOrders(t, repo, ctx)
	})

	t.Run("DuplicateHandling", func(t *testing.T) {
		testDuplicateHandling(t, repo, ctx)
	})

	cleanupTestData(t, repo, ctx)
}

func testCreateOrder(t *testing.T, repo *PostgresRepository, ctx context.Context) {
	order := createTestOrder("create_test_001")

	err := repo.CreateOrder(ctx, order)
	if err != nil {
		t.Fatalf("Failed to create order: %v", err)
	}

	retrieved, err := repo.GetOrderByID(ctx, order.OrderUID)
	if err != nil {
		t.Fatalf("Failed to retrieve created order: %v", err)
	}

	if retrieved == nil {
		t.Fatal("Expected order to be found after creation")
	}

	compareOrders(t, order, retrieved)
}

func testGetOrderByID(t *testing.T, repo *PostgresRepository, ctx context.Context) {
	order := createTestOrder("get_test_001")
	err := repo.CreateOrder(ctx, order)
	if err != nil {
		t.Fatalf("Failed to create test order: %v", err)
	}

	retrieved, err := repo.GetOrderByID(ctx, order.OrderUID)
	if err != nil {
		t.Fatalf("Failed to get order: %v", err)
	}

	if retrieved == nil {
		t.Fatal("Expected order to be found")
	}

	compareOrders(t, order, retrieved)

	nonExistent, err := repo.GetOrderByID(ctx, "non_existent_order")
	if err != nil {
		t.Fatalf("Expected no error for non-existent order, got: %v", err)
	}

	if nonExistent != nil {
		t.Error("Expected nil for non-existent order")
	}
}

func testGetAllOrders(t *testing.T, repo *PostgresRepository, ctx context.Context) {
	orderUIDs := []string{"getall_test_001", "getall_test_002", "getall_test_003", "getall_test_004", "getall_test_005"}

	for _, uid := range orderUIDs {
		order := createTestOrder(uid)
		err := repo.CreateOrder(ctx, order)
		if err != nil {
			t.Fatalf("Failed to create test order %s: %v", uid, err)
		}
	}

	orders, err := repo.GetAllOrders(ctx)
	if err != nil {
		t.Fatalf("Failed to get all orders: %v", err)
	}

	if len(orders) < len(orderUIDs) {
		t.Errorf("Expected at least %d orders, got %d", len(orderUIDs), len(orders))
	}

	foundOrders := make(map[string]*models.Order)
	for _, order := range orders {
		foundOrders[order.OrderUID] = order
	}

	for _, uid := range orderUIDs {
		if foundOrders[uid] == nil {
			t.Errorf("Expected order %s to be found in GetAllOrders result", uid)
		}
	}

	for i := 1; i < len(orders); i++ {
		if orders[i-1].DateCreated.Before(orders[i].DateCreated) {
			t.Error("Expected orders to be sorted by date_created DESC")
			break
		}
	}
}

func testUpdateOrder(t *testing.T, repo *PostgresRepository, ctx context.Context) {
	order := createTestOrder("update_test_001")
	err := repo.CreateOrder(ctx, order)
	if err != nil {
		t.Fatalf("Failed to create initial order: %v", err)
	}

	order.TrackNumber = "UPDATED_TRACK_001"
	order.CustomerID = "updated_customer"
	order.Payment.Amount = 2000
	order.Delivery.Address = "Updated Address 123"

	newItem := models.Item{
		ChrtID:      999999,
		TrackNumber: order.TrackNumber,
		Price:       300,
		RID:         "updated_rid_001",
		Name:        "Updated Item",
		Sale:        15,
		Size:        "XL",
		TotalPrice:  255,
		NMID:        888888,
		Brand:       "Updated Brand",
		Status:      201,
	}
	order.Items = append(order.Items, newItem)

	err = repo.UpdateOrder(ctx, order)
	if err != nil {
		t.Fatalf("Failed to update order: %v", err)
	}

	updated, err := repo.GetOrderByID(ctx, order.OrderUID)
	if err != nil {
		t.Fatalf("Failed to get updated order: %v", err)
	}

	if updated.TrackNumber != "UPDATED_TRACK_001" {
		t.Errorf("Expected TrackNumber to be updated to 'UPDATED_TRACK_001', got %s", updated.TrackNumber)
	}

	if updated.CustomerID != "updated_customer" {
		t.Errorf("Expected CustomerID to be updated to 'updated_customer', got %s", updated.CustomerID)
	}

	if updated.Payment.Amount != 2000 {
		t.Errorf("Expected Payment.Amount to be updated to 2000, got %d", updated.Payment.Amount)
	}

	if len(updated.Items) != len(order.Items) {
		t.Errorf("Expected %d items, got %d", len(order.Items), len(updated.Items))
	}
}

func testDeleteOrder(t *testing.T, repo *PostgresRepository, ctx context.Context) {
	order := createTestOrder("delete_test_001")
	err := repo.CreateOrder(ctx, order)
	if err != nil {
		t.Fatalf("Failed to create order for deletion test: %v", err)
	}

	retrieved, err := repo.GetOrderByID(ctx, order.OrderUID)
	if err != nil {
		t.Fatalf("Failed to get order before deletion: %v", err)
	}
	if retrieved == nil {
		t.Fatal("Expected order to exist before deletion")
	}

	err = repo.DeleteOrder(ctx, order.OrderUID)
	if err != nil {
		t.Fatalf("Failed to delete order: %v", err)
	}

	deleted, err := repo.GetOrderByID(ctx, order.OrderUID)
	if err != nil {
		t.Fatalf("Failed to check if order was deleted: %v", err)
	}
	if deleted != nil {
		t.Error("Expected order to be deleted")
	}

	err = repo.DeleteOrder(ctx, "non_existent_order")
	if err != nil {
		t.Errorf("Expected no error when deleting non-existent order, got: %v", err)
	}
}

func testConcurrentOperations(t *testing.T, repo *PostgresRepository, ctx context.Context) {
	const numWorkers = 5
	const ordersPerWorker = 10

	done := make(chan bool, numWorkers)
	errors := make(chan error, numWorkers*ordersPerWorker)

	for worker := 0; worker < numWorkers; worker++ {
		go func(workerID int) {
			defer func() { done <- true }()

			for i := 0; i < ordersPerWorker; i++ {
				orderUID := fmt.Sprintf("concurrent_worker_%d_order_%03d", workerID, i+1)
				order := createTestOrder(orderUID)

				if err := repo.CreateOrder(ctx, order); err != nil {
					errors <- fmt.Errorf("worker %d order %d: %v", workerID, i+1, err)
				}
			}
		}(worker)
	}

	for i := 0; i < numWorkers; i++ {
		<-done
	}
	close(errors)

	errorCount := 0
	for err := range errors {
		t.Errorf("Concurrent operation error: %v", err)
		errorCount++
	}

	if errorCount > 0 {
		t.Errorf("Had %d errors during concurrent operations", errorCount)
	}

	orders, err := repo.GetAllOrders(ctx)
	if err != nil {
		t.Fatalf("Failed to get orders after concurrent test: %v", err)
	}

	concurrentOrders := 0
	for _, order := range orders {
		if strings.Contains(order.OrderUID, "concurrent_worker_") {
			concurrentOrders++
		}
	}

	expectedOrders := numWorkers * ordersPerWorker
	if concurrentOrders < expectedOrders {
		t.Errorf("Expected at least %d concurrent orders, found %d", expectedOrders, concurrentOrders)
	}

	t.Logf("Successfully created %d orders concurrently", concurrentOrders)
}

func testLargeOrders(t *testing.T, repo *PostgresRepository, ctx context.Context) {
	order := &models.Order{
		OrderUID:          "large_order_test_001",
		TrackNumber:       "LARGE_TRACK_001",
		Entry:             "WBIL",
		Locale:            "en",
		InternalSignature: strings.Repeat("signature", 100),
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
			Address: strings.Repeat("Very long address ", 50),
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

	order.Items = make([]models.Item, 100)
	for i := 0; i < 100; i++ {
		order.Items[i] = models.Item{
			ChrtID:      1000 + i,
			TrackNumber: order.TrackNumber,
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

	start := time.Now()
	err := repo.CreateOrder(ctx, order)
	createDuration := time.Since(start)

	if err != nil {
		t.Fatalf("Failed to create large order: %v", err)
	}

	t.Logf("Large order creation time: %v", createDuration)

	start = time.Now()
	retrieved, err := repo.GetOrderByID(ctx, order.OrderUID)
	getDuration := time.Since(start)

	if err != nil {
		t.Fatalf("Failed to retrieve large order: %v", err)
	}

	t.Logf("Large order retrieval time: %v", getDuration)

	if retrieved == nil {
		t.Fatal("Expected large order to be found")
	}

	if len(retrieved.Items) != len(order.Items) {
		t.Errorf("Expected %d items, got %d", len(order.Items), len(retrieved.Items))
	}

	if retrieved.InternalSignature != order.InternalSignature {
		t.Error("Large signature field was not preserved correctly")
	}

	if retrieved.Delivery.Address != order.Delivery.Address {
		t.Error("Large address field was not preserved correctly")
	}
}

func testDuplicateHandling(t *testing.T, repo *PostgresRepository, ctx context.Context) {
	order := createTestOrder("duplicate_test_001")

	err := repo.CreateOrder(ctx, order)
	if err != nil {
		t.Fatalf("Failed to create initial order: %v", err)
	}

	order.CustomerID = "updated_duplicate_customer"
	order.Payment.Amount = 3000

	err = repo.CreateOrder(ctx, order)
	if err != nil {
		t.Fatalf("Failed to handle duplicate order: %v", err)
	}

	retrieved, err := repo.GetOrderByID(ctx, order.OrderUID)
	if err != nil {
		t.Fatalf("Failed to retrieve order after duplicate handling: %v", err)
	}

	if retrieved.CustomerID != "updated_duplicate_customer" {
		t.Errorf("Expected CustomerID to be updated to 'updated_duplicate_customer', got %s", retrieved.CustomerID)
	}

	if retrieved.Payment.Amount != 3000 {
		t.Errorf("Expected Payment.Amount to be updated to 3000, got %d", retrieved.Payment.Amount)
	}

	orders, err := repo.GetAllOrders(ctx)
	if err != nil {
		t.Fatalf("Failed to get all orders: %v", err)
	}

	duplicateCount := 0
	for _, o := range orders {
		if o.OrderUID == order.OrderUID {
			duplicateCount++
		}
	}

	if duplicateCount != 1 {
		t.Errorf("Expected exactly 1 order with UID %s, found %d", order.OrderUID, duplicateCount)
	}
}

func createTestOrder(orderUID string) *models.Order {
	return &models.Order{
		OrderUID:          orderUID,
		TrackNumber:       fmt.Sprintf("TRACK_%s", orderUID),
		Entry:             "WBIL",
		Locale:            "en",
		InternalSignature: fmt.Sprintf("sig_%s", orderUID),
		CustomerID:        fmt.Sprintf("customer_%s", orderUID),
		DeliveryService:   "meest",
		ShardKey:          "1",
		SMID:              1,
		DateCreated:       time.Now(),
		OOFShard:          "1",
		Delivery: models.Delivery{
			Name:    fmt.Sprintf("Customer %s", orderUID),
			Phone:   "+380501234567",
			Zip:     "01001",
			City:    "Kyiv",
			Address: fmt.Sprintf("Test Street %s", orderUID),
			Region:  "Kyivska",
			Email:   fmt.Sprintf("test_%s@example.com", orderUID),
		},
		Payment: models.Payment{
			Transaction:  fmt.Sprintf("txn_%s", orderUID),
			RequestID:    "",
			Currency:     "USD",
			Provider:     "wbpay",
			Amount:       1000,
			PaymentDT:    time.Now().Unix(),
			Bank:         "alpha",
			DeliveryCost: 200,
			GoodsTotal:   800,
			CustomFee:    0,
		},
		Items: []models.Item{
			{
				ChrtID:      123456,
				TrackNumber: fmt.Sprintf("TRACK_%s", orderUID),
				Price:       400,
				RID:         fmt.Sprintf("rid_%s", orderUID),
				Name:        fmt.Sprintf("Test Item %s", orderUID),
				Sale:        10,
				Size:        "M",
				TotalPrice:  360,
				NMID:        654321,
				Brand:       "Test Brand",
				Status:      202,
			},
			{
				ChrtID:      123457,
				TrackNumber: fmt.Sprintf("TRACK_%s", orderUID),
				Price:       600,
				RID:         fmt.Sprintf("rid_%s_2", orderUID),
				Name:        fmt.Sprintf("Test Item 2 %s", orderUID),
				Sale:        20,
				Size:        "L",
				TotalPrice:  480,
				NMID:        654322,
				Brand:       "Test Brand 2",
				Status:      201,
			},
		},
	}
}

func compareOrders(t *testing.T, expected, actual *models.Order) {
	if actual.OrderUID != expected.OrderUID {
		t.Errorf("Expected OrderUID %s, got %s", expected.OrderUID, actual.OrderUID)
	}

	if actual.TrackNumber != expected.TrackNumber {
		t.Errorf("Expected TrackNumber %s, got %s", expected.TrackNumber, actual.TrackNumber)
	}

	if actual.CustomerID != expected.CustomerID {
		t.Errorf("Expected CustomerID %s, got %s", expected.CustomerID, actual.CustomerID)
	}

	if actual.Delivery.Name != expected.Delivery.Name {
		t.Errorf("Expected Delivery.Name %s, got %s", expected.Delivery.Name, actual.Delivery.Name)
	}

	if actual.Delivery.Email != expected.Delivery.Email {
		t.Errorf("Expected Delivery.Email %s, got %s", expected.Delivery.Email, actual.Delivery.Email)
	}

	if actual.Payment.Amount != expected.Payment.Amount {
		t.Errorf("Expected Payment.Amount %d, got %d", expected.Payment.Amount, actual.Payment.Amount)
	}

	if actual.Payment.Currency != expected.Payment.Currency {
		t.Errorf("Expected Payment.Currency %s, got %s", expected.Payment.Currency, actual.Payment.Currency)
	}

	if len(actual.Items) != len(expected.Items) {
		t.Errorf("Expected %d items, got %d", len(expected.Items), len(actual.Items))
		return
	}

	for i, expectedItem := range expected.Items {
		if i >= len(actual.Items) {
			break
		}
		actualItem := actual.Items[i]

		if actualItem.ChrtID != expectedItem.ChrtID {
			t.Errorf("Item %d: Expected ChrtID %d, got %d", i, expectedItem.ChrtID, actualItem.ChrtID)
		}

		if actualItem.Name != expectedItem.Name {
			t.Errorf("Item %d: Expected Name %s, got %s", i, expectedItem.Name, actualItem.Name)
		}

		if actualItem.Price != expectedItem.Price {
			t.Errorf("Item %d: Expected Price %d, got %d", i, expectedItem.Price, actualItem.Price)
		}
	}
}

func cleanupTestData(t *testing.T, repo *PostgresRepository, ctx context.Context) {
	cfg := &config.DatabaseConfig{
		Host:     "localhost",
		Port:     "5432",
		User:     "orderservice",
		Password: "password",
		DBName:   "orderservice",
		SSLMode:  "disable",
	}
	db, err := sql.Open("postgres", cfg.DSN())
	if err != nil {
		t.Logf("Warning: Could not open database for cleanup: %v", err)
		return
	}
	defer db.Close()

	testPrefixes := []string{
		"create_test_",
		"get_test_",
		"getall_test_",
		"update_test_",
		"delete_test_",
		"concurrent_worker_",
		"large_order_test_",
		"duplicate_test_",
	}

	for _, prefix := range testPrefixes {
		_, err := db.ExecContext(ctx, "DELETE FROM orders WHERE order_uid LIKE $1", prefix+"%")
		if err != nil {
			t.Logf("Warning: Could not clean up test data with prefix %s: %v", prefix, err)
		}
	}
}

func BenchmarkPostgresRepository_CreateOrder(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark tests in short mode")
	}

	cfg := &config.DatabaseConfig{
		Host:     "localhost",
		Port:     "5432",
		User:     "orderservice",
		Password: "password",
		DBName:   "orderservice",
		SSLMode:  "disable",
	}

	repo, err := NewPostgresRepository(cfg)
	if err != nil {
		b.Skipf("Skipping benchmark: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	orders := make([]*models.Order, b.N)
	for i := 0; i < b.N; i++ {
		orders[i] = createTestOrder(fmt.Sprintf("bench_create_%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := repo.CreateOrder(ctx, orders[i])
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	for i := 0; i < b.N; i++ {
		repo.DeleteOrder(ctx, orders[i].OrderUID)
	}
}

func BenchmarkPostgresRepository_GetOrderByID(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark tests in short mode")
	}

	cfg := &config.DatabaseConfig{
		Host:     "localhost",
		Port:     "5432",
		User:     "orderservice",
		Password: "password",
		DBName:   "orderservice",
		SSLMode:  "disable",
	}

	repo, err := NewPostgresRepository(cfg)
	if err != nil {
		b.Skipf("Skipping benchmark: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	numOrders := 1000
	orderUIDs := make([]string, numOrders)
	for i := 0; i < numOrders; i++ {
		orderUID := fmt.Sprintf("bench_get_%d", i)
		orderUIDs[i] = orderUID
		order := createTestOrder(orderUID)
		err := repo.CreateOrder(ctx, order)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		orderUID := orderUIDs[i%numOrders]
		_, err := repo.GetOrderByID(ctx, orderUID)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	for _, orderUID := range orderUIDs {
		repo.DeleteOrder(ctx, orderUID)
	}
}
