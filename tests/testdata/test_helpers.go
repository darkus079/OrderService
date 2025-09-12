package testdata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"orderservice/internal/config"
	"orderservice/internal/database"
	"orderservice/internal/models"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type TestHelper struct {
	cfg  *config.Config
	repo database.Repository
}

func NewTestHelper() *TestHelper {
	cfg := config.TestConfig()
	return &TestHelper{
		cfg: cfg,
	}
}

func (th *TestHelper) SetupDatabase() (database.Repository, error) {
	repo, err := database.NewPostgresRepository(&th.cfg.Database)
	if err != nil {
		return nil, fmt.Errorf("failed to setup test database: %w", err)
	}
	th.repo = repo
	return repo, nil
}

func (th *TestHelper) CleanupDatabase(ctx context.Context) error {
	if th.repo == nil {
		return fmt.Errorf("database not initialized")
	}

	return th.truncateOrders(ctx)
}

func (th *TestHelper) truncateOrders(ctx context.Context) error {
	db, err := sql.Open("postgres", th.cfg.Database.TestDSN())
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, "TRUNCATE TABLE orders")
	return err
}

func (th *TestHelper) GetTestOrders() []*models.Order {
	orders := make([]*models.Order, 0, 25)
	baseTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)

	for i := 0; i < 25; i++ {
		order := &models.Order{
			OrderUID:          fmt.Sprintf("test_order_%03d", i+1),
			TrackNumber:       fmt.Sprintf("TRACK_%03d", i+1),
			Entry:             "WBIL",
			Locale:            "en",
			InternalSignature: "",
			CustomerID:        fmt.Sprintf("customer_%d", (i%5)+1),
			DeliveryService:   []string{"meest", "nova_poshta", "ups", "fedex"}[i%4],
			ShardKey:          fmt.Sprintf("%d", (i%10)+1),
			SMID:              i%100 + 1,
			DateCreated:       baseTime.Add(time.Duration(i) * time.Hour),
			OOFShard:          fmt.Sprintf("%d", (i%3)+1),
		}

		order.Delivery = models.Delivery{
			Name:    fmt.Sprintf("Customer %d", i+1),
			Phone:   fmt.Sprintf("+380%08d", 500000000+i),
			Zip:     fmt.Sprintf("%05d", 10000+i),
			City:    []string{"Kyiv", "Kharkiv", "Odessa", "Dnipro", "Lviv"}[i%5],
			Address: fmt.Sprintf("Street %d, House %d", i%50+1, i%20+1),
			Region:  []string{"Kyivska", "Kharkivska", "Odeska", "Dnipropetrovska", "Lvivska"}[i%5],
			Email:   fmt.Sprintf("customer%d@test.com", i+1),
		}

		order.Payment = models.Payment{
			Transaction:  fmt.Sprintf("txn_%s", order.OrderUID),
			RequestID:    "",
			Currency:     []string{"USD", "EUR", "UAH"}[i%3],
			Provider:     []string{"wbpay", "stripe", "paypal"}[i%3],
			Amount:       (i%10+1)*100 + 500, // 600-1500
			PaymentDT:    baseTime.Add(time.Duration(i-1) * time.Hour).Unix(),
			Bank:         []string{"alpha", "privat", "mono", "oschad"}[i%4],
			DeliveryCost: (i%5 + 1) * 50,     // 50-250
			GoodsTotal:   (i%10+1)*100 + 250, // 350-1250
			CustomFee:    (i % 3) * 25,       // 0, 25, 50
		}

		itemCount := (i % 3) + 1
		order.Items = make([]models.Item, itemCount)

		for j := 0; j < itemCount; j++ {
			order.Items[j] = models.Item{
				ChrtID:      (i+1)*1000 + j + 1,
				TrackNumber: order.TrackNumber,
				Price:       (j+1)*100 + (i%5)*50,
				RID:         fmt.Sprintf("rid_%s_%d", order.OrderUID, j+1),
				Name:        fmt.Sprintf("Product %d-%d", i+1, j+1),
				Sale:        (i + j) % 20,
				Size:        []string{"XS", "S", "M", "L", "XL"}[(i+j)%5],
				TotalPrice:  ((j+1)*100 + (i%5)*50) * (100 - (i+j)%20) / 100,
				NMID:        (i+1)*10000 + j + 1,
				Brand:       fmt.Sprintf("Brand_%d", (i%5)+1),
				Status:      []int{200, 201, 202, 204}[i%4],
			}
		}

		orders = append(orders, order)
	}

	return orders
}

func (th *TestHelper) GetLargeTestOrder() *models.Order {
	order := &models.Order{
		OrderUID:          "large_test_order_001",
		TrackNumber:       "LARGE_TRACK_001",
		Entry:             "WBIL",
		Locale:            "en",
		InternalSignature: "",
		CustomerID:        "large_customer",
		DeliveryService:   "meest",
		ShardKey:          "1",
		SMID:              1,
		DateCreated:       time.Now(),
		OOFShard:          "1",
	}

	order.Delivery = models.Delivery{
		Name:    "Large Order Customer",
		Phone:   "+380501234567",
		Zip:     "01001",
		City:    "Kyiv",
		Address: "Independence Square 1",
		Region:  "Kyivska",
		Email:   "large@test.com",
	}

	order.Payment = models.Payment{
		Transaction:  "large_txn_001",
		RequestID:    "",
		Currency:     "USD",
		Provider:     "wbpay",
		Amount:       50000, // Large amount
		PaymentDT:    time.Now().Unix(),
		Bank:         "alpha",
		DeliveryCost: 500,
		GoodsTotal:   49500,
		CustomFee:    0,
	}

	order.Items = make([]models.Item, 100)
	for i := 0; i < 100; i++ {
		order.Items[i] = models.Item{
			ChrtID:      1000000 + i,
			TrackNumber: order.TrackNumber,
			Price:       (i%10 + 1) * 50,
			RID:         fmt.Sprintf("large_rid_%03d", i+1),
			Name:        fmt.Sprintf("Large Product Item %d with very long description that makes JSON bigger", i+1),
			Sale:        i % 50,
			Size:        fmt.Sprintf("Size_%d", i%10),
			TotalPrice:  ((i%10 + 1) * 50) * (100 - i%50) / 100,
			NMID:        2000000 + i,
			Brand:       fmt.Sprintf("Large Brand %d", (i%20)+1),
			Status:      202,
		}
	}

	return order
}

func (th *TestHelper) GetInvalidTestOrders() []*models.Order {
	orders := []*models.Order{
		{
			OrderUID:    "",
			TrackNumber: "INVALID_TRACK_001",
			Entry:       "WBIL",
		},
		{
			OrderUID:    "invalid_order_001",
			TrackNumber: "",
			Entry:       "WBIL",
		},
		{
			OrderUID:    "invalid_order_002",
			TrackNumber: "INVALID_TRACK_002",
			Entry:       "WBIL",
		},
	}

	return orders
}

func (th *TestHelper) SendKafkaMessage(ctx context.Context, order *models.Order) error {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: th.cfg.Kafka.Brokers,
		Topic:   th.cfg.Kafka.Topic,
	})
	defer writer.Close()

	orderJSON, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("failed to marshal order: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(order.OrderUID),
		Value: orderJSON,
	}

	return writer.WriteMessages(ctx, msg)
}

func (th *TestHelper) SendInvalidKafkaMessage(ctx context.Context, invalidJSON string) error {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: th.cfg.Kafka.Brokers,
		Topic:   th.cfg.Kafka.Topic,
	})
	defer writer.Close()

	msg := kafka.Message{
		Key:   []byte("invalid_message"),
		Value: []byte(invalidJSON),
	}

	return writer.WriteMessages(ctx, msg)
}

func (th *TestHelper) WaitForCondition(ctx context.Context, condition func() bool, timeout time.Duration, checkInterval time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if condition() {
				return nil
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("condition not met within timeout %v", timeout)
			}
		}
	}
}

func (th *TestHelper) LogTestResult(testName string, success bool, duration time.Duration, details string) {
	status := "PASS"
	if !success {
		status = "FAIL"
	}

	log.Printf("[TEST] %s: %s (%.2fms) - %s", testName, status, float64(duration.Nanoseconds())/1e6, details)
}

func (th *TestHelper) GetConfig() *config.Config {
	return th.cfg
}
