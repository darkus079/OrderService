package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"orderservice/internal/config"
	"orderservice/internal/models"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader  *kafka.Reader
	orderCh chan *models.Order
	errorCh chan error
	config  *config.KafkaConfig
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewConsumer(cfg *config.KafkaConfig) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())

	startOffset := kafka.LastOffset
	// Use FirstOffset for test environment to ensure all messages are read
	if config.IsTestEnvironment() {
		startOffset = kafka.FirstOffset
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    startOffset,
	})

	return &Consumer{
		reader:  reader,
		orderCh: make(chan *models.Order, 100),
		errorCh: make(chan error, 100),
		config:  cfg,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (c *Consumer) Start() {
	go c.consume()
}

func (c *Consumer) Stop() {
	c.cancel()
	c.reader.Close()
}

func (c *Consumer) OrderChannel() <-chan *models.Order {
	return c.orderCh
}

func (c *Consumer) ErrorChannel() <-chan error {
	return c.errorCh
}

func (c *Consumer) consume() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			msg, err := c.reader.ReadMessage(c.ctx)
			if err != nil {
				if err == context.Canceled {
					return
				}
				select {
				case c.errorCh <- fmt.Errorf("failed to read message: %w", err):
				case <-c.ctx.Done():
					return
				}
				continue
			}

			order, err := c.parseMessage(msg.Value)
			if err != nil {
				log.Printf("Failed to parse message: %v", err)
				select {
				case c.errorCh <- err:
				case <-c.ctx.Done():
					return
				}
				continue
			}

			select {
			case c.orderCh <- order:
			case <-c.ctx.Done():
				return
			default:
				log.Printf("Order channel is full, dropping message for order: %s", order.OrderUID)
			}
		}
	}
}

func (c *Consumer) parseMessage(data []byte) (*models.Order, error) {
	// First, validate and clean the JSON data
	cleanData := cleanInvalidBytes(data)
	if !utf8.Valid(cleanData) {
		return nil, fmt.Errorf("invalid UTF-8 data")
	}

	var order models.Order
	if err := json.Unmarshal(cleanData, &order); err != nil {
		return nil, fmt.Errorf("failed to unmarshal order: %w", err)
	}

	// Clean and validate string fields
	order = cleanOrderStrings(order)

	if order.OrderUID == "" {
		return nil, fmt.Errorf("order_uid is required")
	}

	if order.TrackNumber == "" {
		return nil, fmt.Errorf("track_number is required")
	}

	if order.DateCreated.IsZero() {
		order.DateCreated = time.Now()
	}

	return &order, nil
}

// cleanInvalidBytes removes null bytes and other problematic characters
func cleanInvalidBytes(data []byte) []byte {
	result := make([]byte, 0, len(data))
	for _, b := range data {
		if b != 0 { // Remove null bytes
			result = append(result, b)
		}
	}
	return result
}

// cleanOrderStrings cleans all string fields in the order
func cleanOrderStrings(order models.Order) models.Order {
	order.OrderUID = cleanString(order.OrderUID)
	order.TrackNumber = cleanString(order.TrackNumber)
	order.Entry = cleanString(order.Entry)
	order.Locale = cleanString(order.Locale)
	order.InternalSignature = cleanString(order.InternalSignature)
	order.CustomerID = cleanString(order.CustomerID)
	order.DeliveryService = cleanString(order.DeliveryService)
	order.ShardKey = cleanString(order.ShardKey)
	order.OOFShard = cleanString(order.OOFShard)

	// Clean delivery fields
	order.Delivery.Name = cleanString(order.Delivery.Name)
	order.Delivery.Phone = cleanString(order.Delivery.Phone)
	order.Delivery.Zip = cleanString(order.Delivery.Zip)
	order.Delivery.City = cleanString(order.Delivery.City)
	order.Delivery.Address = cleanString(order.Delivery.Address)
	order.Delivery.Region = cleanString(order.Delivery.Region)
	order.Delivery.Email = cleanString(order.Delivery.Email)

	// Clean payment fields
	order.Payment.Transaction = cleanString(order.Payment.Transaction)
	order.Payment.RequestID = cleanString(order.Payment.RequestID)
	order.Payment.Currency = cleanString(order.Payment.Currency)
	order.Payment.Provider = cleanString(order.Payment.Provider)
	order.Payment.Bank = cleanString(order.Payment.Bank)

	// Clean item fields
	for i := range order.Items {
		order.Items[i].TrackNumber = cleanString(order.Items[i].TrackNumber)
		order.Items[i].RID = cleanString(order.Items[i].RID)
		order.Items[i].Name = cleanString(order.Items[i].Name)
		order.Items[i].Size = cleanString(order.Items[i].Size)
		order.Items[i].Brand = cleanString(order.Items[i].Brand)
	}

	return order
}

// cleanString removes null bytes and ensures valid UTF-8
func cleanString(s string) string {
	// Remove null bytes and other control characters
	s = strings.ReplaceAll(s, "\x00", "")

	// If string is not valid UTF-8, convert it
	if !utf8.ValidString(s) {
		// Convert to valid UTF-8 by replacing invalid sequences
		result := make([]rune, 0, len(s))
		for _, r := range s {
			if r != utf8.RuneError {
				result = append(result, r)
			}
		}
		s = string(result)
	}

	return s
}

func (c *Consumer) Commit() error {
	return nil
}
