package cache

import (
	"context"
	"orderservice/internal/models"
	"sync"
	"time"
)

// CacheItem represents an item in the cache with metadata
type CacheItem struct {
	Order     *models.Order
	CreatedAt time.Time
	LastUsed  time.Time
}

// Cache implements an in-memory cache for orders
type Cache struct {
	mu      sync.RWMutex
	orders  map[string]*CacheItem
	maxSize int
	repo    Repository
}

// Repository interface for database operations
type Repository interface {
	GetOrderByID(ctx context.Context, orderUID string) (*models.Order, error)
	GetAllOrders(ctx context.Context) ([]*models.Order, error)
}

// NewCache creates a new cache instance
func NewCache(maxSize int, repo Repository) *Cache {
	return &Cache{
		orders:  make(map[string]*CacheItem),
		maxSize: maxSize,
		repo:    repo,
	}
}

// Get retrieves an order from cache or database
func (c *Cache) Get(ctx context.Context, orderUID string) (*models.Order, error) {
	c.mu.RLock()
	item, exists := c.orders[orderUID]
	c.mu.RUnlock()

	if exists {
		// Update last used time
		c.mu.Lock()
		item.LastUsed = time.Now()
		c.mu.Unlock()
		return item.Order, nil
	}

	// Not in cache, fetch from database
	order, err := c.repo.GetOrderByID(ctx, orderUID)
	if err != nil {
		return nil, err
	}

	if order != nil {
		c.Set(orderUID, order)
	}

	return order, nil
}

// Set stores an order in the cache
func (c *Cache) Set(orderUID string, order *models.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if we need to evict items
	if len(c.orders) >= c.maxSize && c.orders[orderUID] == nil {
		c.evictLRU()
	}

	c.orders[orderUID] = &CacheItem{
		Order:     order,
		CreatedAt: time.Now(),
		LastUsed:  time.Now(),
	}
}

// Delete removes an order from the cache
func (c *Cache) Delete(orderUID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.orders, orderUID)
}

// evictLRU removes the least recently used item
func (c *Cache) evictLRU() {
	var oldestKey string
	var oldestTime time.Time

	for key, item := range c.orders {
		if oldestKey == "" || item.LastUsed.Before(oldestTime) {
			oldestKey = key
			oldestTime = item.LastUsed
		}
	}

	if oldestKey != "" {
		delete(c.orders, oldestKey)
	}
}

// LoadFromDB loads all orders from database into cache
func (c *Cache) LoadFromDB(ctx context.Context) error {
	orders, err := c.repo.GetAllOrders(ctx)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear existing cache
	c.orders = make(map[string]*CacheItem)

	// Load orders into cache
	for _, order := range orders {
		c.orders[order.OrderUID] = &CacheItem{
			Order:     order,
			CreatedAt: time.Now(),
			LastUsed:  time.Now(),
		}
	}

	return nil
}

// Size returns the current cache size
func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.orders)
}

// Clear removes all items from the cache
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.orders = make(map[string]*CacheItem)
}
