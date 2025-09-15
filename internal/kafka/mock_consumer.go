package kafka

import (
	"context"
	"fmt"
	"orderservice/internal/config"
	"orderservice/internal/models"
	"time"
)

type MockConsumer struct {
	orderCh chan *models.Order
	errorCh chan error
	config  *config.KafkaConfig
	ctx     context.Context
	cancel  context.CancelFunc
	running bool
}

func NewMockConsumer(cfg *config.KafkaConfig) *MockConsumer {
	ctx, cancel := context.WithCancel(context.Background())

	return &MockConsumer{
		orderCh: make(chan *models.Order, 100),
		errorCh: make(chan error, 100),
		config:  cfg,
		ctx:     ctx,
		cancel:  cancel,
		running: false,
	}
}

func (c *MockConsumer) Start() {
	if c.running {
		return
	}
	c.running = true

	fmt.Println("🔄 Mock Kafka: Starting consumer simulation...")
	go c.simulateMessages()
}

func (c *MockConsumer) Stop() {
	if !c.running {
		return
	}

	fmt.Println("⏹️ Mock Kafka: Stopping consumer...")
	c.running = false
	c.cancel()
	close(c.orderCh)
	close(c.errorCh)
}

func (c *MockConsumer) OrderChannel() <-chan *models.Order {
	return c.orderCh
}

func (c *MockConsumer) ErrorChannel() <-chan error {
	return c.errorCh
}

func (c *MockConsumer) simulateMessages() {
	time.Sleep(3 * time.Second)

	if !c.running {
		return
	}

	demoOrder := &models.Order{
		OrderUID:    fmt.Sprintf("demo-order-%d", time.Now().Unix()),
		TrackNumber: fmt.Sprintf("TRACK%d", time.Now().Unix()),
		Entry:       "DEMO",
		Delivery: models.Delivery{
			Name:    "Demo Customer",
			Phone:   "+1-555-0123",
			Zip:     "12345",
			City:    "Demo City",
			Address: "123 Demo Street",
			Region:  "Demo Region",
			Email:   "demo@example.com",
		},
		Payment: models.Payment{
			Transaction:  fmt.Sprintf("demo-txn-%d", time.Now().Unix()),
			RequestID:    "",
			Currency:     "USD",
			Provider:     "demo-provider",
			Amount:       1999,
			PaymentDT:    time.Now().Unix(),
			Bank:         "demo-bank",
			DeliveryCost: 299,
			GoodsTotal:   1700,
			CustomFee:    0,
		},
		Items: []models.Item{
			{
				ChrtID:      int(time.Now().Unix()),
				TrackNumber: fmt.Sprintf("TRACK%d", time.Now().Unix()),
				Price:       1700,
				RID:         fmt.Sprintf("demo-rid-%d", time.Now().Unix()),
				Name:        "Demo Product",
				Sale:        15,
				Size:        "M",
				TotalPrice:  1445, // After 15% sale
				NMID:        12345,
				Brand:       "Demo Brand",
				Status:      202,
			},
		},
		Locale:            "en",
		InternalSignature: "",
		CustomerID:        "demo-customer",
		DeliveryService:   "demo-delivery",
		ShardKey:          "1",
		SMID:              1,
		DateCreated:       time.Now(),
		OOFShard:          "1",
	}

	fmt.Printf("📨 Mock Kafka: Sending demo order %s\n", demoOrder.OrderUID)

	select {
	case c.orderCh <- demoOrder:
		fmt.Printf("✅ Mock Kafka: Demo order %s sent successfully\n", demoOrder.OrderUID)
	case <-c.ctx.Done():
		return
	default:
		fmt.Printf("⚠️ Mock Kafka: Order channel is full, dropping demo order %s\n", demoOrder.OrderUID)
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	counter := 1
	for {
		select {
		case <-c.ctx.Done():
			fmt.Println("🛑 Mock Kafka: Context cancelled, stopping simulation")
			return
		case <-ticker.C:
			if !c.running {
				return
			}

			periodicOrder := &models.Order{
				OrderUID:    fmt.Sprintf("periodic-order-%d-%d", counter, time.Now().Unix()),
				TrackNumber: fmt.Sprintf("PERIODIC%d", time.Now().Unix()),
				Entry:       "AUTO",
				Delivery: models.Delivery{
					Name:    fmt.Sprintf("Customer %d", counter),
					Phone:   fmt.Sprintf("+1-555-%04d", 1000+counter),
					City:    "Auto City",
					Address: fmt.Sprintf("%d Auto Street", counter),
					Email:   fmt.Sprintf("customer%d@example.com", counter),
				},
				Payment: models.Payment{
					Transaction: fmt.Sprintf("auto-txn-%d-%d", counter, time.Now().Unix()),
					Currency:    "USD",
					Provider:    "auto-provider",
					Amount:      999 + (counter * 100),
					PaymentDT:   time.Now().Unix(),
				},
				Items: []models.Item{
					{
						Name:       fmt.Sprintf("Auto Product %d", counter),
						Price:      999 + (counter * 100),
						TotalPrice: 999 + (counter * 100),
						Brand:      "Auto Brand",
						Status:     202,
					},
				},
				CustomerID:  fmt.Sprintf("auto-customer-%d", counter),
				DateCreated: time.Now(),
			}

			fmt.Printf("🔄 Mock Kafka: Sending periodic order %s\n", periodicOrder.OrderUID)

			select {
			case c.orderCh <- periodicOrder:
				fmt.Printf("✅ Mock Kafka: Periodic order %s sent\n", periodicOrder.OrderUID)
			case <-c.ctx.Done():
				return
			default:
				fmt.Printf("⚠️ Mock Kafka: Channel full, dropping order %s\n", periodicOrder.OrderUID)
			}

			counter++
		}
	}
}

func (c *MockConsumer) SendTestOrder() error {
	if !c.running {
		return fmt.Errorf("consumer not running")
	}

	testOrder := &models.Order{
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

	fmt.Printf("🧪 Mock Kafka: Sending test order %s\n", testOrder.OrderUID)

	select {
	case c.orderCh <- testOrder:
		fmt.Printf("✅ Mock Kafka: Test order sent successfully\n")
		return nil
	case <-c.ctx.Done():
		return fmt.Errorf("context cancelled")
	default:
		return fmt.Errorf("order channel is full")
	}
}

func (c *MockConsumer) Commit() error {
	return nil
}
