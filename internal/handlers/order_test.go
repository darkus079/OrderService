package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"orderservice/internal/models"
	"testing"
	"time"
)

type mockCache struct {
	orders map[string]*models.Order
}

func newMockCache() *mockCache {
	return &mockCache{
		orders: make(map[string]*models.Order),
	}
}

func (m *mockCache) Get(ctx context.Context, orderUID string) (*models.Order, error) {
	if order, exists := m.orders[orderUID]; exists {
		return order, nil
	}
	return nil, nil
}

func (m *mockCache) Set(orderUID string, order *models.Order) {
	m.orders[orderUID] = order
}

func (m *mockCache) Delete(orderUID string) {
	delete(m.orders, orderUID)
}

func (m *mockCache) Size() int {
	return len(m.orders)
}

func (m *mockCache) LoadFromDB(ctx context.Context) error {
	return nil
}

func (m *mockCache) Clear() {
	m.orders = make(map[string]*models.Order)
}

func TestOrderHandler_GetOrder(t *testing.T) {
	mockCache := newMockCache()
	handler := NewOrderHandler(mockCache)

	testOrder := &models.Order{
		OrderUID:    "test-order-1",
		TrackNumber: "TRACK123",
		CustomerID:  "customer1",
		Delivery: models.Delivery{
			Name:  "Test User",
			Email: "test@example.com",
		},
		Payment: models.Payment{
			Amount:   1000,
			Currency: "USD",
		},
		Items: []models.Item{
			{
				Name:  "Test Item",
				Price: 500,
			},
		},
		DateCreated: time.Now(),
	}

	mockCache.Set(testOrder.OrderUID, testOrder)

	tests := []struct {
		name           string
		orderUID       string
		expectedStatus int
		expectError    bool
	}{
		{
			name:           "valid order",
			orderUID:       "test-order-1",
			expectedStatus: http.StatusOK,
			expectError:    false,
		},
		{
			name:           "non-existent order",
			orderUID:       "non-existent",
			expectedStatus: http.StatusNotFound,
			expectError:    true,
		},
		{
			name:           "empty order UID",
			orderUID:       "",
			expectedStatus: http.StatusBadRequest,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "/order/"+tt.orderUID, nil)
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}

			rr := httptest.NewRecorder()
			handler.GetOrder(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, rr.Code)
			}

			if !tt.expectError {
				var response models.Order
				if err := json.NewDecoder(rr.Body).Decode(&response); err != nil {
					t.Errorf("Failed to decode response: %v", err)
				}

				if response.OrderUID != tt.orderUID {
					t.Errorf("Expected OrderUID %s, got %s", tt.orderUID, response.OrderUID)
				}
			} else {
				if rr.Body.String() == "" {
					t.Error("Expected error message in response body")
				}
			}
		})
	}
}

func TestOrderHandler_HealthCheck(t *testing.T) {
	mockCache := newMockCache()
	handler := NewOrderHandler(mockCache)

	req, err := http.NewRequest("GET", "/health", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	rr := httptest.NewRecorder()
	handler.HealthCheck(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, rr.Code)
	}

	contentType := rr.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type application/json, got %s", contentType)
	}

	var response map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&response); err != nil {
		t.Errorf("Failed to decode response: %v", err)
	}

	if response["status"] != "healthy" {
		t.Errorf("Expected status 'healthy', got %v", response["status"])
	}

	if response["cache_size"] == nil {
		t.Error("Expected cache_size in response")
	}
}

func TestOrderHandler_GetOrderStats(t *testing.T) {
	mockCache := newMockCache()
	handler := NewOrderHandler(mockCache)

	testOrder := &models.Order{
		OrderUID:    "test-order-1",
		TrackNumber: "TRACK123",
		CustomerID:  "customer1",
	}
	mockCache.Set(testOrder.OrderUID, testOrder)

	req, err := http.NewRequest("GET", "/stats", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	rr := httptest.NewRecorder()
	handler.GetOrderStats(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, rr.Code)
	}

	contentType := rr.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type application/json, got %s", contentType)
	}

	var response map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&response); err != nil {
		t.Errorf("Failed to decode response: %v", err)
	}

	if response["cache_size"] != float64(1) {
		t.Errorf("Expected cache_size 1, got %v", response["cache_size"])
	}

	if response["timestamp"] == nil {
		t.Error("Expected timestamp in response")
	}
}

func TestOrderHandler_GetOrderWithComplexData(t *testing.T) {
	mockCache := newMockCache()
	handler := NewOrderHandler(mockCache)

	complexOrder := &models.Order{
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

	mockCache.Set(complexOrder.OrderUID, complexOrder)

	req, err := http.NewRequest("GET", "/order/"+complexOrder.OrderUID, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	rr := httptest.NewRecorder()
	handler.GetOrder(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, rr.Code)
	}

	var response models.Order
	if err := json.NewDecoder(rr.Body).Decode(&response); err != nil {
		t.Errorf("Failed to decode response: %v", err)
	}

	if response.OrderUID != complexOrder.OrderUID {
		t.Errorf("Expected OrderUID %s, got %s", complexOrder.OrderUID, response.OrderUID)
	}

	if response.Delivery.Name != complexOrder.Delivery.Name {
		t.Errorf("Expected Delivery.Name %s, got %s", complexOrder.Delivery.Name, response.Delivery.Name)
	}

	if response.Payment.Amount != complexOrder.Payment.Amount {
		t.Errorf("Expected Payment.Amount %d, got %d", complexOrder.Payment.Amount, response.Payment.Amount)
	}

	if len(response.Items) != len(complexOrder.Items) {
		t.Errorf("Expected %d items, got %d", len(complexOrder.Items), len(response.Items))
	}

	if len(response.Items) > 0 && response.Items[0].Name != complexOrder.Items[0].Name {
		t.Errorf("Expected first item name %s, got %s", complexOrder.Items[0].Name, response.Items[0].Name)
	}
}
