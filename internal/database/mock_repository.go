package database

import (
	"context"
	"fmt"
	"orderservice/internal/models"
	"sync"
	"time"
)

// MockRepository implements Repository interface for testing without PostgreSQL
type MockRepository struct {
	orders map[string]*models.Order
	mutex  sync.RWMutex
}

// NewMockRepository creates a new mock repository with sample data
func NewMockRepository() *MockRepository {
	repo := &MockRepository{
		orders: make(map[string]*models.Order),
	}

	// Add sample order from model.json
	sampleOrder := &models.Order{
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

	repo.orders[sampleOrder.OrderUID] = sampleOrder
	return repo
}

// CreateOrder creates a new order in memory
func (r *MockRepository) CreateOrder(ctx context.Context, order *models.Order) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Simulate database constraint validation
	if order.OrderUID == "" {
		return fmt.Errorf("order_uid cannot be empty")
	}

	if order.TrackNumber == "" {
		return fmt.Errorf("track_number cannot be empty")
	}

	// Store order
	r.orders[order.OrderUID] = order
	fmt.Printf("📦 Mock DB: Saved order %s\n", order.OrderUID)

	return nil
}

// GetOrderByID retrieves an order by its ID
func (r *MockRepository) GetOrderByID(ctx context.Context, orderUID string) (*models.Order, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	order, exists := r.orders[orderUID]
	if !exists {
		return nil, nil // Not found
	}

	fmt.Printf("🔍 Mock DB: Retrieved order %s\n", orderUID)
	return order, nil
}

// GetAllOrders retrieves all orders
func (r *MockRepository) GetAllOrders(ctx context.Context) ([]*models.Order, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	var orders []*models.Order
	for _, order := range r.orders {
		orders = append(orders, order)
	}

	fmt.Printf("📋 Mock DB: Retrieved %d orders\n", len(orders))
	return orders, nil
}

// UpdateOrder updates an existing order
func (r *MockRepository) UpdateOrder(ctx context.Context, order *models.Order) error {
	return r.CreateOrder(ctx, order) // Reuse create logic
}

// DeleteOrder deletes an order by ID
func (r *MockRepository) DeleteOrder(ctx context.Context, orderUID string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	delete(r.orders, orderUID)
	fmt.Printf("🗑️ Mock DB: Deleted order %s\n", orderUID)
	return nil
}

// Close closes the repository (no-op for mock)
func (r *MockRepository) Close() error {
	fmt.Println("🔌 Mock DB: Connection closed")
	return nil
}

// AddSampleOrders adds more sample orders for testing
func (r *MockRepository) AddSampleOrders() {
	sampleOrders := []*models.Order{
		{
			OrderUID:    "order-001",
			TrackNumber: "TRACK001",
			Entry:       "DEMO",
			Delivery: models.Delivery{
				Name:    "John Doe",
				Phone:   "+1234567890",
				City:    "New York",
				Address: "123 Main St",
				Email:   "john@example.com",
			},
			Payment: models.Payment{
				Transaction: "txn-001",
				Currency:    "USD",
				Amount:      2500,
				Provider:    "demo-pay",
			},
			Items: []models.Item{
				{
					Name:       "Demo Product",
					Price:      2500,
					TotalPrice: 2500,
					Brand:      "Demo Brand",
				},
			},
			CustomerID:  "demo-customer",
			DateCreated: time.Now(),
		},
		{
			OrderUID:    "order-002",
			TrackNumber: "TRACK002",
			Entry:       "DEMO",
			Delivery: models.Delivery{
				Name:    "Jane Smith",
				Phone:   "+0987654321",
				City:    "Los Angeles",
				Address: "456 Oak Ave",
				Email:   "jane@example.com",
			},
			Payment: models.Payment{
				Transaction: "txn-002",
				Currency:    "USD",
				Amount:      1500,
				Provider:    "demo-pay",
			},
			Items: []models.Item{
				{
					Name:       "Another Product",
					Price:      1500,
					TotalPrice: 1500,
					Brand:      "Cool Brand",
				},
			},
			CustomerID:  "demo-customer-2",
			DateCreated: time.Now(),
		},
	}

	for _, order := range sampleOrders {
		r.CreateOrder(context.Background(), order)
	}
}

// GetStats returns repository statistics
func (r *MockRepository) GetStats() map[string]interface{} {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return map[string]interface{}{
		"total_orders": len(r.orders),
		"type":         "mock_repository",
		"status":       "active",
	}
}
