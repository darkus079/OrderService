package cache

import (
	"context"
	"fmt"
	"orderservice/internal/models"
	"testing"
	"time"
)

type mockRepository struct {
	orders map[string]*models.Order
}

func newMockRepository() *mockRepository {
	return &mockRepository{
		orders: make(map[string]*models.Order),
	}
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

func (m *mockRepository) addOrder(order *models.Order) {
	m.orders[order.OrderUID] = order
}

func TestCache_SetAndGet(t *testing.T) {
	repo := newMockRepository()
	cache := NewCache(100, repo)

	order := &models.Order{
		OrderUID:    "test-order-1",
		TrackNumber: "TRACK123",
		CustomerID:  "customer1",
	}

	cache.Set(order.OrderUID, order)

	retrieved, err := cache.Get(context.Background(), order.OrderUID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if retrieved == nil {
		t.Fatal("Expected order to be found in cache")
	}

	if retrieved.OrderUID != order.OrderUID {
		t.Errorf("Expected OrderUID %s, got %s", order.OrderUID, retrieved.OrderUID)
	}
}

func TestCache_GetFromDatabase(t *testing.T) {
	repo := newMockRepository()
	cache := NewCache(100, repo)

	order := &models.Order{
		OrderUID:    "test-order-1",
		TrackNumber: "TRACK123",
		CustomerID:  "customer1",
	}

	repo.addOrder(order)

	retrieved, err := cache.Get(context.Background(), order.OrderUID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if retrieved == nil {
		t.Fatal("Expected order to be found")
	}

	if retrieved.OrderUID != order.OrderUID {
		t.Errorf("Expected OrderUID %s, got %s", order.OrderUID, retrieved.OrderUID)
	}

	if cache.Size() != 1 {
		t.Errorf("Expected cache size 1, got %d", cache.Size())
	}
}

func TestCache_NotFound(t *testing.T) {
	repo := newMockRepository()
	cache := NewCache(100, repo)

	retrieved, err := cache.Get(context.Background(), "non-existent")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if retrieved != nil {
		t.Error("Expected nil for non-existent order")
	}
}

func TestCache_Delete(t *testing.T) {
	repo := newMockRepository()
	cache := NewCache(100, repo)

	order := &models.Order{
		OrderUID:    "test-order-1",
		TrackNumber: "TRACK123",
		CustomerID:  "customer1",
	}

	cache.Set(order.OrderUID, order)

	if cache.Size() != 1 {
		t.Errorf("Expected cache size 1, got %d", cache.Size())
	}

	cache.Delete(order.OrderUID)

	if cache.Size() != 0 {
		t.Errorf("Expected cache size 0, got %d", cache.Size())
	}
}

func TestCache_LoadFromDB(t *testing.T) {
	repo := newMockRepository()
	cache := NewCache(100, repo)

	orders := []*models.Order{
		{OrderUID: "order-1", TrackNumber: "TRACK1", CustomerID: "customer1"},
		{OrderUID: "order-2", TrackNumber: "TRACK2", CustomerID: "customer2"},
		{OrderUID: "order-3", TrackNumber: "TRACK3", CustomerID: "customer3"},
	}

	for _, order := range orders {
		repo.addOrder(order)
	}

	err := cache.LoadFromDB(context.Background())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if cache.Size() != 3 {
		t.Errorf("Expected cache size 3, got %d", cache.Size())
	}

	for _, expectedOrder := range orders {
		retrieved, err := cache.Get(context.Background(), expectedOrder.OrderUID)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if retrieved == nil {
			t.Errorf("Expected order %s to be found", expectedOrder.OrderUID)
		}
	}
}

func TestCache_Eviction(t *testing.T) {
	repo := newMockRepository()
	cache := NewCache(2, repo)

	orders := []*models.Order{
		{OrderUID: "order-1", TrackNumber: "TRACK1", CustomerID: "customer1"},
		{OrderUID: "order-2", TrackNumber: "TRACK2", CustomerID: "customer2"},
		{OrderUID: "order-3", TrackNumber: "TRACK3", CustomerID: "customer3"},
	}

	for _, order := range orders {
		cache.Set(order.OrderUID, order)
	}

	if cache.Size() > 2 {
		t.Errorf("Cache size %d exceeds max size 2", cache.Size())
	}

	accessible := 0
	for _, order := range orders {
		if retrieved, _ := cache.Get(context.Background(), order.OrderUID); retrieved != nil {
			accessible++
		}
	}

	if accessible == 0 {
		t.Error("No orders are accessible from cache")
	}
}

func TestCache_Clear(t *testing.T) {
	repo := newMockRepository()
	cache := NewCache(100, repo)

	order := &models.Order{
		OrderUID:    "test-order-1",
		TrackNumber: "TRACK123",
		CustomerID:  "customer1",
	}

	cache.Set(order.OrderUID, order)

	if cache.Size() != 1 {
		t.Errorf("Expected cache size 1, got %d", cache.Size())
	}

	cache.Clear()

	if cache.Size() != 0 {
		t.Errorf("Expected cache size 0, got %d", cache.Size())
	}
}

func TestCache_LRUEviction(t *testing.T) {
	repo := newMockRepository()
	cache := NewCache(3, repo)

	orders := []*models.Order{
		{OrderUID: "order-1", TrackNumber: "TRACK1", CustomerID: "customer1"},
		{OrderUID: "order-2", TrackNumber: "TRACK2", CustomerID: "customer2"},
		{OrderUID: "order-3", TrackNumber: "TRACK3", CustomerID: "customer3"},
		{OrderUID: "order-4", TrackNumber: "TRACK4", CustomerID: "customer4"},
	}

	for i := 0; i < 3; i++ {
		cache.Set(orders[i].OrderUID, orders[i])
		time.Sleep(1 * time.Millisecond)
	}

	if cache.Size() != 3 {
		t.Errorf("Expected cache size 3, got %d", cache.Size())
	}

	_, err := cache.Get(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	time.Sleep(1 * time.Millisecond)

	_, err = cache.Get(context.Background(), "order-2")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	time.Sleep(1 * time.Millisecond)

	cache.Set(orders[3].OrderUID, orders[3])

	if cache.Size() != 3 {
		t.Errorf("Expected cache size 3, got %d", cache.Size())
	}

	retrieved, err := cache.Get(context.Background(), "order-3")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if retrieved != nil {
		t.Error("Expected order-3 to be evicted from cache")
	}

	for _, orderUID := range []string{"order-1", "order-2", "order-4"} {
		retrieved, err := cache.Get(context.Background(), orderUID)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if retrieved == nil {
			t.Errorf("Expected %s to still be in cache", orderUID)
		}
	}
}

func TestCache_OverflowHandling(t *testing.T) {
	repo := newMockRepository()
	maxSize := 10
	cache := NewCache(maxSize, repo)

	for i := 0; i < maxSize; i++ {
		order := &models.Order{
			OrderUID:    fmt.Sprintf("order-%03d", i+1),
			TrackNumber: fmt.Sprintf("TRACK%03d", i+1),
			CustomerID:  fmt.Sprintf("customer%d", (i%5)+1),
		}
		cache.Set(order.OrderUID, order)
		time.Sleep(1 * time.Millisecond)
	}

	if cache.Size() != maxSize {
		t.Errorf("Expected cache size %d, got %d", maxSize, cache.Size())
	}

	overflowOrders := 5
	for i := maxSize; i < maxSize+overflowOrders; i++ {
		order := &models.Order{
			OrderUID:    fmt.Sprintf("order-%03d", i+1),
			TrackNumber: fmt.Sprintf("TRACK%03d", i+1),
			CustomerID:  fmt.Sprintf("customer%d", (i%5)+1),
		}
		cache.Set(order.OrderUID, order)
		time.Sleep(1 * time.Millisecond)
	}

	if cache.Size() != maxSize {
		t.Errorf("Cache size %d exceeds max capacity %d", cache.Size(), maxSize)
	}

	for i := overflowOrders; i < maxSize+overflowOrders; i++ {
		orderUID := fmt.Sprintf("order-%03d", i+1)
		retrieved, err := cache.Get(context.Background(), orderUID)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if retrieved == nil {
			t.Errorf("Expected recent order %s to be in cache", orderUID)
		}
	}

	for i := 0; i < overflowOrders; i++ {
		orderUID := fmt.Sprintf("order-%03d", i+1)
		retrieved, err := cache.Get(context.Background(), orderUID)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if retrieved != nil {
			t.Errorf("Expected old order %s to be evicted from cache", orderUID)
		}
	}
}

func TestCache_ConcurrentAccess(t *testing.T) {
	repo := newMockRepository()
	cache := NewCache(100, repo)

	for i := 0; i < 50; i++ {
		order := &models.Order{
			OrderUID:    fmt.Sprintf("order-%03d", i+1),
			TrackNumber: fmt.Sprintf("TRACK%03d", i+1),
			CustomerID:  fmt.Sprintf("customer%d", (i%10)+1),
		}
		cache.Set(order.OrderUID, order)
	}

	done := make(chan bool, 10)

	for i := 0; i < 5; i++ {
		go func(workerID int) {
			for j := 0; j < 100; j++ {
				orderUID := fmt.Sprintf("order-%03d", (j%50)+1)
				_, err := cache.Get(context.Background(), orderUID)
				if err != nil {
					t.Errorf("Reader %d: Unexpected error: %v", workerID, err)
				}
			}
			done <- true
		}(i)
	}

	for i := 0; i < 5; i++ {
		go func(workerID int) {
			for j := 0; j < 50; j++ {
				order := &models.Order{
					OrderUID:    fmt.Sprintf("concurrent-%d-%03d", workerID, j+1),
					TrackNumber: fmt.Sprintf("CONC_TRACK_%d_%03d", workerID, j+1),
					CustomerID:  fmt.Sprintf("concurrent_customer_%d", workerID),
				}
				cache.Set(order.OrderUID, order)
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	if cache.Size() == 0 {
		t.Error("Cache should not be empty after concurrent operations")
	}

	if cache.Size() > 100 {
		t.Errorf("Cache size %d exceeds max capacity 100", cache.Size())
	}
}

func TestCache_UpdateExistingOrder(t *testing.T) {
	repo := newMockRepository()
	cache := NewCache(100, repo)

	originalOrder := &models.Order{
		OrderUID:    "update-test-order",
		TrackNumber: "ORIGINAL_TRACK",
		CustomerID:  "original_customer",
	}

	cache.Set(originalOrder.OrderUID, originalOrder)

	updatedOrder := &models.Order{
		OrderUID:    "update-test-order",
		TrackNumber: "UPDATED_TRACK",
		CustomerID:  "updated_customer",
	}

	cache.Set(updatedOrder.OrderUID, updatedOrder)

	if cache.Size() != 1 {
		t.Errorf("Expected cache size 1, got %d", cache.Size())
	}

	retrieved, err := cache.Get(context.Background(), originalOrder.OrderUID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if retrieved == nil {
		t.Fatal("Expected updated order to be found in cache")
	}

	if retrieved.TrackNumber != "UPDATED_TRACK" {
		t.Errorf("Expected TrackNumber to be updated to 'UPDATED_TRACK', got %s", retrieved.TrackNumber)
	}

	if retrieved.CustomerID != "updated_customer" {
		t.Errorf("Expected CustomerID to be updated to 'updated_customer', got %s", retrieved.CustomerID)
	}
}

func TestCache_MemoryUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory usage test in short mode")
	}

	repo := newMockRepository()
	maxSize := 1000
	cache := NewCache(maxSize, repo)

	for i := 0; i < maxSize*2; i++ {
		order := &models.Order{
			OrderUID:          fmt.Sprintf("memory-test-order-%06d", i+1),
			TrackNumber:       fmt.Sprintf("MEMORY_TRACK_%06d", i+1),
			Entry:             "WBIL",
			Locale:            "en",
			InternalSignature: fmt.Sprintf("signature_%d", i),
			CustomerID:        fmt.Sprintf("memory_customer_%d", (i%100)+1),
			DeliveryService:   fmt.Sprintf("service_%d", i%5),
			ShardKey:          fmt.Sprintf("%d", (i%10)+1),
			SMID:              i + 1,
			DateCreated:       time.Now().Add(time.Duration(i) * time.Second),
			OOFShard:          fmt.Sprintf("%d", (i%3)+1),
		}

		itemCount := (i % 5) + 1
		order.Items = make([]models.Item, itemCount)
		for j := 0; j < itemCount; j++ {
			order.Items[j] = models.Item{
				ChrtID:      (i+1)*1000 + j + 1,
				TrackNumber: order.TrackNumber,
				Price:       (j + 1) * 100,
				RID:         fmt.Sprintf("memory_rid_%d_%d", i+1, j+1),
				Name:        fmt.Sprintf("Memory Test Product %d-%d", i+1, j+1),
				Sale:        (i + j) % 50,
				Size:        fmt.Sprintf("Size_%d", j),
				TotalPrice:  ((j + 1) * 100) * (100 - (i+j)%50) / 100,
				NMID:        (i+1)*10000 + j + 1,
				Brand:       fmt.Sprintf("Memory_Brand_%d", (i%20)+1),
				Status:      200 + (i % 5),
			}
		}

		cache.Set(order.OrderUID, order)
	}

	if cache.Size() != maxSize {
		t.Errorf("Expected cache size %d, got %d", maxSize, cache.Size())
	}

	t.Logf("Memory stress test completed. Cache size: %d", cache.Size())
}

func BenchmarkCache_Set(b *testing.B) {
	repo := newMockRepository()
	cache := NewCache(1000, repo)

	orders := make([]*models.Order, b.N)
	for i := 0; i < b.N; i++ {
		orders[i] = &models.Order{
			OrderUID:    fmt.Sprintf("benchmark-order-%d", i),
			TrackNumber: fmt.Sprintf("BENCH_TRACK_%d", i),
			CustomerID:  fmt.Sprintf("benchmark_customer_%d", i%100),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set(orders[i].OrderUID, orders[i])
	}
}

func BenchmarkCache_Get(b *testing.B) {
	repo := newMockRepository()
	cache := NewCache(1000, repo)

	numOrders := 1000
	for i := 0; i < numOrders; i++ {
		order := &models.Order{
			OrderUID:    fmt.Sprintf("benchmark-order-%d", i),
			TrackNumber: fmt.Sprintf("BENCH_TRACK_%d", i),
			CustomerID:  fmt.Sprintf("benchmark_customer_%d", i%100),
		}
		cache.Set(order.OrderUID, order)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		orderUID := fmt.Sprintf("benchmark-order-%d", i%numOrders)
		_, _ = cache.Get(context.Background(), orderUID)
	}
}

func BenchmarkCache_LRUEviction(b *testing.B) {
	repo := newMockRepository()
	cache := NewCache(100, repo)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		order := &models.Order{
			OrderUID:    fmt.Sprintf("eviction-benchmark-order-%d", i),
			TrackNumber: fmt.Sprintf("EVICT_TRACK_%d", i),
			CustomerID:  fmt.Sprintf("eviction_customer_%d", i%10),
		}
		cache.Set(order.OrderUID, order)
	}
}
