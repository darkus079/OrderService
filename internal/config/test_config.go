package config

import (
	"fmt"
	"os"
)

func TestConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Port: getEnv("TEST_SERVER_PORT", "8082"),
			Host: getEnv("TEST_SERVER_HOST", "localhost"),
		},
		Database: DatabaseConfig{
			Host:     getEnv("TEST_DB_HOST", "localhost"),
			Port:     getEnv("TEST_DB_PORT", "5432"),
			User:     getEnv("TEST_DB_USER", "orderservice"),
			Password: getEnv("TEST_DB_PASSWORD", "password"),
			DBName:   getEnv("TEST_DB_NAME", "orderservice"),
			SSLMode:  getEnv("TEST_DB_SSLMODE", "disable"),
		},
		Kafka: KafkaConfig{
			Brokers: []string{getEnv("TEST_KAFKA_BROKERS", "127.0.0.1:9092")},
			Topic:   getEnv("TEST_KAFKA_TOPIC", "orders"),
			GroupID: getEnv("TEST_KAFKA_GROUP_ID", "order-service-test"),
		},
		Cache: CacheConfig{
			MaxSize: getEnvAsInt("TEST_CACHE_MAX_SIZE", 100),
		},
	}
}

func (c *DatabaseConfig) TestDSN() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.DBName, c.SSLMode)
}

func IsTestEnvironment() bool {
	return os.Getenv("GO_ENV") == "test" ||
		os.Getenv("TESTING") == "true" ||
		isRunningTests()
}

func isRunningTests() bool {
	for _, arg := range os.Args {
		if arg == "-test.v" || arg == "-test.run" {
			return true
		}
	}
	return false
}
