package models

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestOrder_JSONMarshaling(t *testing.T) {
	order := &Order{
		OrderUID:          "test_order_123",
		TrackNumber:       "TRACK_123",
		Entry:             "WBIL",
		Locale:            "en",
		InternalSignature: "test_sig",
		CustomerID:        "customer_123",
		DeliveryService:   "meest",
		ShardKey:          "1",
		SMID:              99,
		DateCreated:       time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
		OOFShard:          "1",
		Delivery: Delivery{
			Name:    "Test Customer",
			Phone:   "+380501234567",
			Zip:     "01001",
			City:    "Kyiv",
			Address: "Test Street 1",
			Region:  "Kyivska",
			Email:   "test@example.com",
		},
		Payment: Payment{
			Transaction:  "txn_123",
			RequestID:    "req_123",
			Currency:     "USD",
			Provider:     "wbpay",
			Amount:       1000,
			PaymentDT:    1640995200,
			Bank:         "alpha",
			DeliveryCost: 200,
			GoodsTotal:   800,
			CustomFee:    50,
		},
		Items: []Item{
			{
				ChrtID:      1001,
				TrackNumber: "TRACK_123",
				Price:       500,
				RID:         "rid_123_1",
				Name:        "Test Item 1",
				Sale:        10,
				Size:        "M",
				TotalPrice:  450,
				NMID:        10001,
				Brand:       "Test Brand",
				Status:      202,
			},
		},
	}

	jsonData, err := json.Marshal(order)
	if err != nil {
		t.Fatalf("Failed to marshal order: %v", err)
	}

	var unmarshaled Order
	if err := json.Unmarshal(jsonData, &unmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal order: %v", err)
	}

	if unmarshaled.OrderUID != order.OrderUID {
		t.Errorf("Expected OrderUID %s, got %s", order.OrderUID, unmarshaled.OrderUID)
	}

	if unmarshaled.TrackNumber != order.TrackNumber {
		t.Errorf("Expected TrackNumber %s, got %s", order.TrackNumber, unmarshaled.TrackNumber)
	}

	if unmarshaled.Delivery.Name != order.Delivery.Name {
		t.Errorf("Expected Delivery.Name %s, got %s", order.Delivery.Name, unmarshaled.Delivery.Name)
	}

	if unmarshaled.Payment.Amount != order.Payment.Amount {
		t.Errorf("Expected Payment.Amount %d, got %d", order.Payment.Amount, unmarshaled.Payment.Amount)
	}

	if len(unmarshaled.Items) != len(order.Items) {
		t.Errorf("Expected %d items, got %d", len(order.Items), len(unmarshaled.Items))
	}

	if len(unmarshaled.Items) > 0 {
		if unmarshaled.Items[0].Name != order.Items[0].Name {
			t.Errorf("Expected Item.Name %s, got %s", order.Items[0].Name, unmarshaled.Items[0].Name)
		}
	}
}

func TestOrder_Validation(t *testing.T) {
	tests := []struct {
		name        string
		order       Order
		expectValid bool
		description string
	}{
		{
			name: "Valid Order",
			order: Order{
				OrderUID:    "valid_order_123",
				TrackNumber: "TRACK_123",
				Entry:       "WBIL",
				Delivery: Delivery{
					Name:  "Valid Customer",
					Email: "valid@example.com",
				},
				Payment: Payment{
					Transaction: "txn_123",
					Currency:    "USD",
					Amount:      1000,
				},
				Items: []Item{
					{
						ChrtID: 123,
						Name:   "Valid Item",
					},
				},
			},
			expectValid: true,
			description: "Valid order with all required fields",
		},
		{
			name: "Empty OrderUID",
			order: Order{
				OrderUID:    "",
				TrackNumber: "TRACK_123",
			},
			expectValid: false,
			description: "Order with empty OrderUID should be invalid",
		},
		{
			name: "Empty TrackNumber",
			order: Order{
				OrderUID:    "order_123",
				TrackNumber: "",
			},
			expectValid: false,
			description: "Order with empty TrackNumber should be invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isValid := tt.order.OrderUID != "" && tt.order.TrackNumber != ""

			if isValid != tt.expectValid {
				t.Errorf("Expected validity %v, got %v for %s", tt.expectValid, isValid, tt.description)
			}
		})
	}
}

func TestDelivery_Validation(t *testing.T) {
	tests := []struct {
		name     string
		delivery Delivery
		isValid  bool
	}{
		{
			name: "Valid Delivery",
			delivery: Delivery{
				Name:    "John Doe",
				Phone:   "+380501234567",
				Email:   "john@example.com",
				Address: "Test Street 1",
				City:    "Kyiv",
			},
			isValid: true,
		},
		{
			name: "Empty Name",
			delivery: Delivery{
				Name:    "",
				Phone:   "+380501234567",
				Email:   "john@example.com",
				Address: "Test Street 1",
				City:    "Kyiv",
			},
			isValid: false,
		},
		{
			name: "Invalid Email",
			delivery: Delivery{
				Name:    "John Doe",
				Phone:   "+380501234567",
				Email:   "invalid-email",
				Address: "Test Street 1",
				City:    "Kyiv",
			},
			isValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isValid := tt.delivery.Name != "" &&
				tt.delivery.Email != "" &&
				len(tt.delivery.Email) > 5 &&
				strings.Contains(tt.delivery.Email, "@") &&
				tt.delivery.Address != "" &&
				tt.delivery.City != ""

			if isValid != tt.isValid {
				t.Errorf("Expected %v, got %v", tt.isValid, isValid)
			}
		})
	}
}

func TestPayment_Validation(t *testing.T) {
	tests := []struct {
		name    string
		payment Payment
		isValid bool
	}{
		{
			name: "Valid Payment",
			payment: Payment{
				Transaction: "txn_123",
				Currency:    "USD",
				Provider:    "wbpay",
				Amount:      1000,
				Bank:        "alpha",
			},
			isValid: true,
		},
		{
			name: "Empty Transaction",
			payment: Payment{
				Transaction: "",
				Currency:    "USD",
				Provider:    "wbpay",
				Amount:      1000,
				Bank:        "alpha",
			},
			isValid: false,
		},
		{
			name: "Negative Amount",
			payment: Payment{
				Transaction: "txn_123",
				Currency:    "USD",
				Provider:    "wbpay",
				Amount:      -100,
				Bank:        "alpha",
			},
			isValid: false,
		},
		{
			name: "Zero Amount",
			payment: Payment{
				Transaction: "txn_123",
				Currency:    "USD",
				Provider:    "wbpay",
				Amount:      0,
				Bank:        "alpha",
			},
			isValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isValid := tt.payment.Transaction != "" &&
				tt.payment.Currency != "" &&
				tt.payment.Provider != "" &&
				tt.payment.Amount > 0

			if isValid != tt.isValid {
				t.Errorf("Expected %v, got %v", tt.isValid, isValid)
			}
		})
	}
}

func TestItem_Validation(t *testing.T) {
	tests := []struct {
		name    string
		item    Item
		isValid bool
	}{
		{
			name: "Valid Item",
			item: Item{
				ChrtID:      123,
				TrackNumber: "TRACK_123",
				Price:       500,
				RID:         "rid_123",
				Name:        "Test Item",
				Size:        "M",
				TotalPrice:  450,
				NMID:        10001,
				Brand:       "Test Brand",
				Status:      202,
			},
			isValid: true,
		},
		{
			name: "Zero ChrtID",
			item: Item{
				ChrtID:      0,
				TrackNumber: "TRACK_123",
				Name:        "Test Item",
			},
			isValid: false,
		},
		{
			name: "Empty Name",
			item: Item{
				ChrtID:      123,
				TrackNumber: "TRACK_123",
				Name:        "",
			},
			isValid: false,
		},
		{
			name: "Negative Price",
			item: Item{
				ChrtID:      123,
				TrackNumber: "TRACK_123",
				Price:       -100,
				Name:        "Test Item",
			},
			isValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isValid := tt.item.ChrtID > 0 &&
				tt.item.Name != "" &&
				tt.item.Price >= 0 &&
				tt.item.TrackNumber != ""

			if isValid != tt.isValid {
				t.Errorf("Expected %v, got %v", tt.isValid, isValid)
			}
		})
	}
}

func TestOrder_LargeJSONHandling(t *testing.T) {
	order := &Order{
		OrderUID:        "large_order_test",
		TrackNumber:     "LARGE_TRACK",
		Entry:           "WBIL",
		Locale:          "en",
		CustomerID:      "large_customer",
		DeliveryService: "meest",
		ShardKey:        "1",
		SMID:            1,
		DateCreated:     time.Now(),
		OOFShard:        "1",
		Delivery: Delivery{
			Name:    "Large Order Customer",
			Phone:   "+380501234567",
			Zip:     "01001",
			City:    "Kyiv",
			Address: "Large Street 1",
			Region:  "Kyivska",
			Email:   "large@test.com",
		},
		Payment: Payment{
			Transaction:  "large_txn",
			Currency:     "USD",
			Provider:     "wbpay",
			Amount:       10000,
			PaymentDT:    time.Now().Unix(),
			Bank:         "alpha",
			DeliveryCost: 500,
			GoodsTotal:   9500,
			CustomFee:    0,
		},
	}

	order.Items = make([]Item, 50)
	for i := 0; i < 50; i++ {
		order.Items[i] = Item{
			ChrtID:      1000 + i,
			TrackNumber: order.TrackNumber,
			Price:       (i%10 + 1) * 100,
			RID:         fmt.Sprintf("rid_large_%d", i),
			Name:        fmt.Sprintf("Large Item %d with very long name", i),
			Sale:        i % 20,
			Size:        fmt.Sprintf("Size_%d", i%5),
			TotalPrice:  ((i%10 + 1) * 100) * (100 - i%20) / 100,
			NMID:        10000 + i,
			Brand:       fmt.Sprintf("Brand_%d", i%10),
			Status:      202,
		}
	}

	start := time.Now()
	jsonData, err := json.Marshal(order)
	marshalDuration := time.Since(start)

	if err != nil {
		t.Fatalf("Failed to marshal large order: %v", err)
	}

	t.Logf("Large JSON size: %d bytes, marshal time: %v", len(jsonData), marshalDuration)

	if len(jsonData) < 1000 {
		t.Error("Expected large JSON (>1000 bytes), got smaller")
	}

	start = time.Now()
	var unmarshaled Order
	if err := json.Unmarshal(jsonData, &unmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal large order: %v", err)
	}
	unmarshalDuration := time.Since(start)

	t.Logf("Unmarshal time: %v", unmarshalDuration)

	if unmarshaled.OrderUID != order.OrderUID {
		t.Errorf("Expected OrderUID %s, got %s", order.OrderUID, unmarshaled.OrderUID)
	}

	if len(unmarshaled.Items) != len(order.Items) {
		t.Errorf("Expected %d items, got %d", len(order.Items), len(unmarshaled.Items))
	}

	for i := 0; i < min(5, len(unmarshaled.Items)); i++ {
		if unmarshaled.Items[i].ChrtID != order.Items[i].ChrtID {
			t.Errorf("Item %d: Expected ChrtID %d, got %d", i, order.Items[i].ChrtID, unmarshaled.Items[i].ChrtID)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func BenchmarkOrder_JSONMarshal(b *testing.B) {
	order := &Order{
		OrderUID:    "benchmark_order",
		TrackNumber: "BENCH_TRACK",
		Entry:       "WBIL",
		Delivery: Delivery{
			Name:  "Benchmark Customer",
			Email: "bench@test.com",
		},
		Payment: Payment{
			Transaction: "bench_txn",
			Currency:    "USD",
			Amount:      1000,
		},
		Items: []Item{
			{ChrtID: 1, Name: "Bench Item 1"},
			{ChrtID: 2, Name: "Bench Item 2"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(order)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOrder_JSONUnmarshal(b *testing.B) {
	order := &Order{
		OrderUID:    "benchmark_order",
		TrackNumber: "BENCH_TRACK",
		Entry:       "WBIL",
		Delivery: Delivery{
			Name:  "Benchmark Customer",
			Email: "bench@test.com",
		},
		Payment: Payment{
			Transaction: "bench_txn",
			Currency:    "USD",
			Amount:      1000,
		},
		Items: []Item{
			{ChrtID: 1, Name: "Bench Item 1"},
			{ChrtID: 2, Name: "Bench Item 2"},
		},
	}

	jsonData, err := json.Marshal(order)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var unmarshaled Order
		err := json.Unmarshal(jsonData, &unmarshaled)
		if err != nil {
			b.Fatal(err)
		}
	}
}
