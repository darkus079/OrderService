package database

import (
	"context"
	"orderservice/internal/models"
)

// Repository defines the interface for order data operations
type Repository interface {
	// CreateOrder creates a new order in the database
	CreateOrder(ctx context.Context, order *models.Order) error

	// GetOrderByID retrieves an order by its ID
	GetOrderByID(ctx context.Context, orderUID string) (*models.Order, error)

	// GetAllOrders retrieves all orders (for cache initialization)
	GetAllOrders(ctx context.Context) ([]*models.Order, error)

	// UpdateOrder updates an existing order
	UpdateOrder(ctx context.Context, order *models.Order) error

	// DeleteOrder deletes an order by ID
	DeleteOrder(ctx context.Context, orderUID string) error

	// Close closes the database connection
	Close() error
}
