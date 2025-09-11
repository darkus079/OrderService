package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"orderservice/internal/config"
	"orderservice/internal/models"
	"time"

	"github.com/segmentio/kafka-go"
)

// Consumer handles Kafka message consumption
type Consumer struct {
	reader  *kafka.Reader
	orderCh chan *models.Order
	errorCh chan error
	config  *config.KafkaConfig
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(cfg *config.KafkaConfig) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
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

// Start begins consuming messages from Kafka
func (c *Consumer) Start() {
	go c.consume()
}

// Stop stops the consumer
func (c *Consumer) Stop() {
	c.cancel()
	close(c.orderCh)
	close(c.errorCh)
	c.reader.Close()
}

// OrderChannel returns the channel for receiving orders
func (c *Consumer) OrderChannel() <-chan *models.Order {
	return c.orderCh
}

// ErrorChannel returns the channel for receiving errors
func (c *Consumer) ErrorChannel() <-chan error {
	return c.errorCh
}

// consume reads messages from Kafka and processes them
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
				c.errorCh <- fmt.Errorf("failed to read message: %w", err)
				continue
			}

			// Parse the message
			order, err := c.parseMessage(msg.Value)
			if err != nil {
				log.Printf("Failed to parse message: %v", err)
				c.errorCh <- err
				continue
			}

			// Send order to channel
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

// parseMessage parses a Kafka message into an Order
func (c *Consumer) parseMessage(data []byte) (*models.Order, error) {
	var order models.Order
	if err := json.Unmarshal(data, &order); err != nil {
		return nil, fmt.Errorf("failed to unmarshal order: %w", err)
	}

	// Validate required fields
	if order.OrderUID == "" {
		return nil, fmt.Errorf("order_uid is required")
	}

	if order.TrackNumber == "" {
		return nil, fmt.Errorf("track_number is required")
	}

	// Set default values if not provided
	if order.DateCreated.IsZero() {
		order.DateCreated = time.Now()
	}

	return &order, nil
}

// Commit commits the current offset
func (c *Consumer) Commit() error {
	// In newer versions of kafka-go, commits are handled automatically
	// when using ReaderConfig.CommitInterval
	return nil
}
