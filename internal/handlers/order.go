package handlers

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"orderservice/internal/models"
	"time"
)

// Cache interface for dependency injection
type Cache interface {
	Get(ctx context.Context, orderUID string) (*models.Order, error)
	Size() int
}

// OrderHandler handles HTTP requests for orders
type OrderHandler struct {
	cache Cache
}

// NewOrderHandler creates a new order handler
func NewOrderHandler(cache Cache) *OrderHandler {
	return &OrderHandler{
		cache: cache,
	}
}

// GetOrder handles GET /order/{orderUID} requests
func (h *OrderHandler) GetOrder(w http.ResponseWriter, r *http.Request) {
	// Extract order UID from URL path
	orderUID := r.URL.Path[len("/order/"):]
	if orderUID == "" {
		http.Error(w, "Order UID is required", http.StatusBadRequest)
		return
	}

	// Get order from cache
	order, err := h.cache.Get(r.Context(), orderUID)
	if err != nil {
		log.Printf("Error getting order %s: %v", orderUID, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if order == nil {
		http.Error(w, "Order not found", http.StatusNotFound)
		return
	}

	// Set response headers
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Encode and send response
	if err := json.NewEncoder(w).Encode(order); err != nil {
		log.Printf("Error encoding order %s: %v", orderUID, err)
	}
}

// HealthCheck handles health check requests
func (h *OrderHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status":     "healthy",
		"timestamp":  time.Now().UTC(),
		"cache_size": h.cache.Size(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// GetOrderStats returns cache statistics
func (h *OrderHandler) GetOrderStats(w http.ResponseWriter, r *http.Request) {
	stats := map[string]interface{}{
		"cache_size": h.cache.Size(),
		"timestamp":  time.Now().UTC(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(stats)
}
