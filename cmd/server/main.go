package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"orderservice/internal/cache"
	"orderservice/internal/config"
	"orderservice/internal/database"
	"orderservice/internal/handlers"
	"orderservice/internal/kafka"
)

func main() {
	cfg := config.Load()
	log.Printf("Starting Order Service with config: %+v", cfg)

	repo, err := database.NewPostgresRepository(&cfg.Database)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer repo.Close()

	orderCache := cache.NewCache(cfg.Cache.MaxSize, repo)

	ctx := context.Background()
	if err := orderCache.LoadFromDB(ctx); err != nil {
		log.Printf("Warning: Failed to load orders into cache: %v", err)
	} else {
		log.Printf("Loaded %d orders into cache", orderCache.Size())
	}

	consumer := kafka.NewConsumer(&cfg.Kafka)
	defer consumer.Stop()

	consumer.Start()
	log.Printf("Started Kafka consumer for topic: %s", cfg.Kafka.Topic)

	go processOrders(ctx, consumer, repo, orderCache)

	orderHandler := handlers.NewOrderHandler(orderCache)

	mux := http.NewServeMux()
	mux.HandleFunc("/order/", orderHandler.GetOrder)
	mux.HandleFunc("/health", orderHandler.HealthCheck)
	mux.HandleFunc("/stats", orderHandler.GetOrderStats)
	mux.HandleFunc("/", serveWebInterface)

	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%s", cfg.Server.Host, cfg.Server.Port),
		Handler: mux,
	}

	go func() {
		log.Printf("Starting HTTP server on %s:%s", cfg.Server.Host, cfg.Server.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
}

func processOrders(ctx context.Context, consumer *kafka.Consumer, repo database.Repository, cache *cache.Cache) {
	for {
		select {
		case <-ctx.Done():
			return
		case order := <-consumer.OrderChannel():
			if order != nil {
				log.Printf("Processing order: %s", order.OrderUID)

				if err := repo.CreateOrder(ctx, order); err != nil {
					log.Printf("Failed to save order %s to database: %v", order.OrderUID, err)
					continue
				}

				cache.Set(order.OrderUID, order)
				log.Printf("Order %s processed successfully", order.OrderUID)
			}
		case err := <-consumer.ErrorChannel():
			if err != nil {
				log.Printf("Kafka consumer error: %v", err)
			}
		}
	}
}

func serveWebInterface(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	http.ServeFile(w, r, "web/templates/index.html")
}
