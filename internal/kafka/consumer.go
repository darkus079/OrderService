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

func (c *Consumer) Start() {
	go c.consume()
}

func (c *Consumer) Stop() {
	c.cancel()
	close(c.orderCh)
	close(c.errorCh)
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
				c.errorCh <- fmt.Errorf("failed to read message: %w", err)
				continue
			}

			order, err := c.parseMessage(msg.Value)
			if err != nil {
				log.Printf("Failed to parse message: %v", err)
				c.errorCh <- err
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
	var order models.Order
	if err := json.Unmarshal(data, &order); err != nil {
		return nil, fmt.Errorf("failed to unmarshal order: %w", err)
	}

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

func (c *Consumer) Commit() error {
	return nil
}
