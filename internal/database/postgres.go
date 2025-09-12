package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"orderservice/internal/config"
	"orderservice/internal/models"

	_ "github.com/lib/pq"
)

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(cfg *config.DatabaseConfig) (*PostgresRepository, error) {
	db, err := sql.Open("postgres", cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	repo := &PostgresRepository{db: db}

	if err := repo.createTables(); err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return repo, nil
}

func (r *PostgresRepository) createTables() error {
	query := `
	CREATE TABLE IF NOT EXISTS orders (
		order_uid VARCHAR(255) PRIMARY KEY,
		track_number VARCHAR(255),
		entry VARCHAR(255),
		delivery JSONB,
		payment JSONB,
		items JSONB,
		locale VARCHAR(10),
		internal_signature VARCHAR(255),
		customer_id VARCHAR(255),
		delivery_service VARCHAR(255),
		shard_key VARCHAR(255),
		sm_id INTEGER,
		date_created TIMESTAMP,
		oof_shard VARCHAR(255),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	
	CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
	CREATE INDEX IF NOT EXISTS idx_orders_date_created ON orders(date_created);
	`

	_, err := r.db.Exec(query)
	return err
}

func (r *PostgresRepository) CreateOrder(ctx context.Context, order *models.Order) error {
	deliveryJSON, err := json.Marshal(order.Delivery)
	if err != nil {
		return fmt.Errorf("failed to marshal delivery: %w", err)
	}

	paymentJSON, err := json.Marshal(order.Payment)
	if err != nil {
		return fmt.Errorf("failed to marshal payment: %w", err)
	}

	itemsJSON, err := json.Marshal(order.Items)
	if err != nil {
		return fmt.Errorf("failed to marshal items: %w", err)
	}

	query := `
		INSERT INTO orders (
			order_uid, track_number, entry, delivery, payment, items,
			locale, internal_signature, customer_id, delivery_service,
			shard_key, sm_id, date_created, oof_shard
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (order_uid) DO UPDATE SET
			track_number = EXCLUDED.track_number,
			entry = EXCLUDED.entry,
			delivery = EXCLUDED.delivery,
			payment = EXCLUDED.payment,
			items = EXCLUDED.items,
			locale = EXCLUDED.locale,
			internal_signature = EXCLUDED.internal_signature,
			customer_id = EXCLUDED.customer_id,
			delivery_service = EXCLUDED.delivery_service,
			shard_key = EXCLUDED.shard_key,
			sm_id = EXCLUDED.sm_id,
			date_created = EXCLUDED.date_created,
			oof_shard = EXCLUDED.oof_shard,
			updated_at = CURRENT_TIMESTAMP
	`

	_, err = r.db.ExecContext(ctx, query,
		order.OrderUID, order.TrackNumber, order.Entry, deliveryJSON, paymentJSON, itemsJSON,
		order.Locale, order.InternalSignature, order.CustomerID, order.DeliveryService,
		order.ShardKey, order.SMID, order.DateCreated, order.OOFShard,
	)

	return err
}

func (r *PostgresRepository) GetOrderByID(ctx context.Context, orderUID string) (*models.Order, error) {
	query := `
		SELECT order_uid, track_number, entry, delivery, payment, items,
			   locale, internal_signature, customer_id, delivery_service,
			   shard_key, sm_id, date_created, oof_shard
		FROM orders WHERE order_uid = $1
	`

	var order models.Order
	var deliveryJSON, paymentJSON, itemsJSON []byte

	err := r.db.QueryRowContext(ctx, query, orderUID).Scan(
		&order.OrderUID, &order.TrackNumber, &order.Entry, &deliveryJSON, &paymentJSON, &itemsJSON,
		&order.Locale, &order.InternalSignature, &order.CustomerID, &order.DeliveryService,
		&order.ShardKey, &order.SMID, &order.DateCreated, &order.OOFShard,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	if err := json.Unmarshal(deliveryJSON, &order.Delivery); err != nil {
		return nil, fmt.Errorf("failed to unmarshal delivery: %w", err)
	}

	if err := json.Unmarshal(paymentJSON, &order.Payment); err != nil {
		return nil, fmt.Errorf("failed to unmarshal payment: %w", err)
	}

	if err := json.Unmarshal(itemsJSON, &order.Items); err != nil {
		return nil, fmt.Errorf("failed to unmarshal items: %w", err)
	}

	return &order, nil
}

func (r *PostgresRepository) GetAllOrders(ctx context.Context) ([]*models.Order, error) {
	query := `
		SELECT order_uid, track_number, entry, delivery, payment, items,
			   locale, internal_signature, customer_id, delivery_service,
			   shard_key, sm_id, date_created, oof_shard
		FROM orders ORDER BY date_created DESC
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var orders []*models.Order
	for rows.Next() {
		var order models.Order
		var deliveryJSON, paymentJSON, itemsJSON []byte

		err := rows.Scan(
			&order.OrderUID, &order.TrackNumber, &order.Entry, &deliveryJSON, &paymentJSON, &itemsJSON,
			&order.Locale, &order.InternalSignature, &order.CustomerID, &order.DeliveryService,
			&order.ShardKey, &order.SMID, &order.DateCreated, &order.OOFShard,
		)
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal(deliveryJSON, &order.Delivery); err != nil {
			return nil, fmt.Errorf("failed to unmarshal delivery: %w", err)
		}

		if err := json.Unmarshal(paymentJSON, &order.Payment); err != nil {
			return nil, fmt.Errorf("failed to unmarshal payment: %w", err)
		}

		if err := json.Unmarshal(itemsJSON, &order.Items); err != nil {
			return nil, fmt.Errorf("failed to unmarshal items: %w", err)
		}

		orders = append(orders, &order)
	}

	return orders, rows.Err()
}

func (r *PostgresRepository) UpdateOrder(ctx context.Context, order *models.Order) error {
	return r.CreateOrder(ctx, order) // Using upsert logic
}

func (r *PostgresRepository) DeleteOrder(ctx context.Context, orderUID string) error {
	query := `DELETE FROM orders WHERE order_uid = $1`
	_, err := r.db.ExecContext(ctx, query, orderUID)
	return err
}

func (r *PostgresRepository) Close() error {
	return r.db.Close()
}
