package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"orderservice/internal/models"
	"strings"
	"testing"
	"time"
)

type mockCache struct {
	orders     map[string]*models.Order
	errorOnGet bool
	getError   error
}

func newMockCache() *mockCache {
	return &mockCache{
		orders: make(map[string]*models.Order),
	}
}

func (m *mockCache) Get(ctx context.Context, orderUID string) (*models.Order, error) {
	if m.errorOnGet {
		return nil, m.getError
	}
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

func TestOrderHandler_GetOrderEdgeCases(t *testing.T) {
	tests := []struct {
		name           string
		setupCache     func() *mockCache
		orderUID       string
		expectedStatus int
		expectError    bool
		description    string
	}{
		{
			name: "Cache Error",
			setupCache: func() *mockCache {
				cache := newMockCache()
				cache.errorOnGet = true
				cache.getError = errors.New("cache connection error")
				return cache
			},
			orderUID:       "test-order-1",
			expectedStatus: http.StatusInternalServerError,
			expectError:    true,
			description:    "Should return 500 when cache returns error",
		},
		{
			name: "Very Long Order UID",
			setupCache: func() *mockCache {
				return newMockCache()
			},
			orderUID:       strings.Repeat("a", 1000),
			expectedStatus: http.StatusNotFound,
			expectError:    true,
			description:    "Should handle very long order UIDs",
		},
		{
			name: "Order UID with Special Characters",
			setupCache: func() *mockCache {
				cache := newMockCache()
				orderUID := "order-with-special-chars-!@#$%^&*()"
				order := &models.Order{
					OrderUID:    orderUID,
					TrackNumber: "SPECIAL_TRACK",
					CustomerID:  "special_customer",
				}
				cache.Set(orderUID, order)
				return cache
			},
			orderUID:       "order-with-special-chars-!@#$%^&*()",
			expectedStatus: http.StatusOK,
			expectError:    false,
			description:    "Should handle order UIDs with special characters",
		},
		{
			name: "Order UID with Unicode",
			setupCache: func() *mockCache {
				cache := newMockCache()
				orderUID := "заказ-тест-123-🛍️"
				order := &models.Order{
					OrderUID:    orderUID,
					TrackNumber: "UNICODE_TRACK",
					CustomerID:  "unicode_customer",
				}
				cache.Set(orderUID, order)
				return cache
			},
			orderUID:       "заказ-тест-123-🛍️",
			expectedStatus: http.StatusOK,
			expectError:    false,
			description:    "Should handle Unicode order UIDs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := tt.setupCache()
			handler := NewOrderHandler(cache)

			req, err := http.NewRequest("GET", "/order/"+url.PathEscape(tt.orderUID), nil)
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}

			rr := httptest.NewRecorder()
			handler.GetOrder(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("%s: Expected status %d, got %d", tt.description, tt.expectedStatus, rr.Code)
			}

			if tt.expectError && rr.Body.String() == "" {
				t.Errorf("%s: Expected error message in response body", tt.description)
			}

			if !tt.expectError {
				contentType := rr.Header().Get("Content-Type")
				if contentType != "application/json" {
					t.Errorf("%s: Expected Content-Type application/json, got %s", tt.description, contentType)
				}
			}
		})
	}
}

func TestOrderHandler_HTTPHeaders(t *testing.T) {
	mockCache := newMockCache()
	handler := NewOrderHandler(mockCache)

	testOrder := &models.Order{
		OrderUID:    "header-test-order",
		TrackNumber: "HEADER_TRACK",
		CustomerID:  "header_customer",
	}
	mockCache.Set(testOrder.OrderUID, testOrder)

	tests := []struct {
		name     string
		endpoint string
		handler  http.HandlerFunc
	}{
		{
			name:     "GetOrder",
			endpoint: "/order/" + testOrder.OrderUID,
			handler:  handler.GetOrder,
		},
		{
			name:     "HealthCheck",
			endpoint: "/health",
			handler:  handler.HealthCheck,
		},
		{
			name:     "GetOrderStats",
			endpoint: "/stats",
			handler:  handler.GetOrderStats,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", tt.endpoint, nil)
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}

			rr := httptest.NewRecorder()
			tt.handler(rr, req)

			contentType := rr.Header().Get("Content-Type")
			if contentType != "application/json" {
				t.Errorf("Expected Content-Type application/json, got %s", contentType)
			}

			var jsonResponse interface{}
			if err := json.NewDecoder(rr.Body).Decode(&jsonResponse); err != nil {
				t.Errorf("Response is not valid JSON: %v", err)
			}
		})
	}
}

func TestOrderHandler_ConcurrentRequests(t *testing.T) {
	mockCache := newMockCache()
	handler := NewOrderHandler(mockCache)

	for i := 0; i < 100; i++ {
		order := &models.Order{
			OrderUID:    fmt.Sprintf("concurrent-order-%03d", i+1),
			TrackNumber: fmt.Sprintf("CONCURRENT_TRACK_%03d", i+1),
			CustomerID:  fmt.Sprintf("concurrent_customer_%d", (i%10)+1),
		}
		mockCache.Set(order.OrderUID, order)
	}

	const numWorkers = 10
	const requestsPerWorker = 50
	done := make(chan bool, numWorkers)
	results := make(chan int, numWorkers*requestsPerWorker)

	for worker := 0; worker < numWorkers; worker++ {
		go func(workerID int) {
			defer func() { done <- true }()

			for i := 0; i < requestsPerWorker; i++ {
				orderUID := fmt.Sprintf("concurrent-order-%03d", (i%100)+1)

				req, err := http.NewRequest("GET", "/order/"+orderUID, nil)
				if err != nil {
					results <- http.StatusInternalServerError
					continue
				}

				rr := httptest.NewRecorder()
				handler.GetOrder(rr, req)
				results <- rr.Code
			}
		}(worker)
	}

	for i := 0; i < numWorkers; i++ {
		<-done
	}
	close(results)

	successCount := 0
	errorCount := 0
	for status := range results {
		if status == http.StatusOK {
			successCount++
		} else {
			errorCount++
		}
	}

	expectedTotal := numWorkers * requestsPerWorker
	actualTotal := successCount + errorCount

	if actualTotal != expectedTotal {
		t.Errorf("Expected %d total requests, got %d", expectedTotal, actualTotal)
	}

	if successCount == 0 {
		t.Error("Expected some successful requests")
	}

	t.Logf("Concurrent test results: %d successful, %d errors out of %d total",
		successCount, errorCount, actualTotal)
}

func TestOrderHandler_LargeOrderData(t *testing.T) {
	mockCache := newMockCache()
	handler := NewOrderHandler(mockCache)

	largeOrder := &models.Order{
		OrderUID:          "large-data-order-001",
		TrackNumber:       "LARGE_TRACK_001",
		Entry:             "WBIL",
		Locale:            "en",
		InternalSignature: strings.Repeat("signature", 100), // Large signature
		CustomerID:        "large_data_customer",
		DeliveryService:   "meest",
		ShardKey:          "1",
		SMID:              1,
		DateCreated:       time.Now(),
		OOFShard:          "1",
		Delivery: models.Delivery{
			Name:    "Large Data Customer",
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

	largeOrder.Items = make([]models.Item, 50)
	for i := 0; i < 50; i++ {
		largeOrder.Items[i] = models.Item{
			ChrtID:      1000 + i,
			TrackNumber: largeOrder.TrackNumber,
			Price:       (i%10 + 1) * 100,
			RID:         fmt.Sprintf("large_rid_%03d", i+1),
			Name: fmt.Sprintf("Large Product Item %d with very long description %s",
				i+1, strings.Repeat("details ", 20)),
			Sale:       i % 50,
			Size:       fmt.Sprintf("Size_%d", i%10),
			TotalPrice: ((i%10 + 1) * 100) * (100 - i%50) / 100,
			NMID:       2000 + i,
			Brand:      fmt.Sprintf("Large Brand %d", (i%20)+1),
			Status:     202,
		}
	}

	mockCache.Set(largeOrder.OrderUID, largeOrder)

	req, err := http.NewRequest("GET", "/order/"+largeOrder.OrderUID, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	rr := httptest.NewRecorder()
	start := time.Now()
	handler.GetOrder(rr, req)
	duration := time.Since(start)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, rr.Code)
	}

	responseSize := len(rr.Body.Bytes())
	t.Logf("Large order response size: %d bytes, processing time: %v", responseSize, duration)

	if responseSize < 10000 {
		t.Error("Expected large response size (>10KB)")
	}

	var response models.Order
	if err := json.NewDecoder(rr.Body).Decode(&response); err != nil {
		t.Errorf("Failed to decode large order response: %v", err)
	}

	if response.OrderUID != largeOrder.OrderUID {
		t.Errorf("Expected OrderUID %s, got %s", largeOrder.OrderUID, response.OrderUID)
	}

	if len(response.Items) != len(largeOrder.Items) {
		t.Errorf("Expected %d items, got %d", len(largeOrder.Items), len(response.Items))
	}
}

func TestOrderHandler_MethodNotAllowed(t *testing.T) {
	mockCache := newMockCache()
	handler := NewOrderHandler(mockCache)

	methods := []string{"POST", "PUT", "DELETE", "PATCH"}
	endpoints := []string{
		"/order/test-order-1",
		"/health",
		"/stats",
	}

	handlerMap := map[string]http.HandlerFunc{
		"/order/test-order-1": handler.GetOrder,
		"/health":             handler.HealthCheck,
		"/stats":              handler.GetOrderStats,
	}

	for _, method := range methods {
		for _, endpoint := range endpoints {
			t.Run(fmt.Sprintf("%s_%s", method, strings.Replace(endpoint, "/", "_", -1)), func(t *testing.T) {
				req, err := http.NewRequest(method, endpoint, nil)
				if err != nil {
					t.Fatalf("Failed to create request: %v", err)
				}

				rr := httptest.NewRecorder()
				handlerMap[endpoint](rr, req)

				if rr.Code >= 500 {
					t.Errorf("Handler returned server error for %s %s: %d", method, endpoint, rr.Code)
				}
			})
		}
	}
}

func TestOrderHandler_EmptyCache(t *testing.T) {
	mockCache := newMockCache() // Empty cache
	handler := NewOrderHandler(mockCache)

	tests := []struct {
		name           string
		endpoint       string
		handler        http.HandlerFunc
		expectedStatus int
	}{
		{
			name:           "GetOrder with empty cache",
			endpoint:       "/order/non-existent-order",
			handler:        handler.GetOrder,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "HealthCheck with empty cache",
			endpoint:       "/health",
			handler:        handler.HealthCheck,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "GetOrderStats with empty cache",
			endpoint:       "/stats",
			handler:        handler.GetOrderStats,
			expectedStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", tt.endpoint, nil)
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}

			rr := httptest.NewRecorder()
			tt.handler(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, rr.Code)
			}

			if tt.name == "HealthCheck with empty cache" || tt.name == "GetOrderStats with empty cache" {
				var response map[string]interface{}
				if err := json.NewDecoder(rr.Body).Decode(&response); err != nil {
					t.Errorf("Failed to decode response: %v", err)
				}

				if cacheSize, ok := response["cache_size"].(float64); ok {
					if cacheSize != 0 {
						t.Errorf("Expected cache_size 0, got %v", cacheSize)
					}
				}
			}
		})
	}
}

func BenchmarkOrderHandler_GetOrder(b *testing.B) {
	mockCache := newMockCache()
	handler := NewOrderHandler(mockCache)

	for i := 0; i < 1000; i++ {
		order := &models.Order{
			OrderUID:    fmt.Sprintf("benchmark-order-%d", i),
			TrackNumber: fmt.Sprintf("BENCH_TRACK_%d", i),
			CustomerID:  fmt.Sprintf("benchmark_customer_%d", i%100),
		}
		mockCache.Set(order.OrderUID, order)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		orderUID := fmt.Sprintf("benchmark-order-%d", i%1000)
		req, _ := http.NewRequest("GET", "/order/"+orderUID, nil)
		rr := httptest.NewRecorder()
		handler.GetOrder(rr, req)
	}
}

func BenchmarkOrderHandler_HealthCheck(b *testing.B) {
	mockCache := newMockCache()
	handler := NewOrderHandler(mockCache)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest("GET", "/health", nil)
		rr := httptest.NewRecorder()
		handler.HealthCheck(rr, req)
	}
}
