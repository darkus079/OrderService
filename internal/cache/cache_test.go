package cache

import (
	"context"
	"orderservice/internal/models"
	"testing"
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
