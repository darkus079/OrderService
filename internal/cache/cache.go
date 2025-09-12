package cache

import (
	"context"
	"orderservice/internal/models"
	"sync"
	"time"
)

type CacheItem struct {
	Order     *models.Order
	CreatedAt time.Time
	LastUsed  time.Time
}

type Cache struct {
	mu      sync.RWMutex
	orders  map[string]*CacheItem
	maxSize int
	repo    Repository
}

type Repository interface {
	GetOrderByID(ctx context.Context, orderUID string) (*models.Order, error)
	GetAllOrders(ctx context.Context) ([]*models.Order, error)
}

func NewCache(maxSize int, repo Repository) *Cache {
	return &Cache{
		orders:  make(map[string]*CacheItem),
		maxSize: maxSize,
		repo:    repo,
	}
}

func (c *Cache) Get(ctx context.Context, orderUID string) (*models.Order, error) {
	c.mu.RLock()
	item, exists := c.orders[orderUID]
	c.mu.RUnlock()

	if exists {
		c.mu.Lock()
		item.LastUsed = time.Now()
		c.mu.Unlock()
		return item.Order, nil
	}

	order, err := c.repo.GetOrderByID(ctx, orderUID)
	if err != nil {
		return nil, err
	}

	if order != nil {
		c.Set(orderUID, order)
	}

	return order, nil
}

func (c *Cache) Set(orderUID string, order *models.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.orders) >= c.maxSize && c.orders[orderUID] == nil {
		c.evictLRU()
	}

	c.orders[orderUID] = &CacheItem{
		Order:     order,
		CreatedAt: time.Now(),
		LastUsed:  time.Now(),
	}
}

func (c *Cache) Delete(orderUID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.orders, orderUID)
}

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

func (c *Cache) LoadFromDB(ctx context.Context) error {
	orders, err := c.repo.GetAllOrders(ctx)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.orders = make(map[string]*CacheItem)

	for _, order := range orders {
		c.orders[order.OrderUID] = &CacheItem{
			Order:     order,
			CreatedAt: time.Now(),
			LastUsed:  time.Now(),
		}
	}

	return nil
}

func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.orders)
}

func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.orders = make(map[string]*CacheItem)
}
