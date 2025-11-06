package infra

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/sirupsen/logrus"
)

// ClickHouseManager manages ClickHouse cluster lifecycle
type ClickHouseManager interface {
	Start(ctx context.Context) error
	Stop() error
	HealthCheck(ctx context.Context) error
	CleanupEphemeralDatabases(ctx context.Context, maxAge time.Duration) error
}

type clickhouseManager struct {
	dockerManager DockerManager
	connStr       string
	log           logrus.FieldLogger

	conn *sql.DB
}

const (
	healthCheckTimeout = 60 * time.Second
	healthCheckRetries = 12
	healthCheckInterval = 5 * time.Second
)

// NewClickHouseManager creates a new ClickHouse cluster manager
func NewClickHouseManager(dockerManager DockerManager, connStr string, log logrus.FieldLogger) ClickHouseManager {
	return &clickhouseManager{
		dockerManager: dockerManager,
		connStr:       connStr,
		log:           log.WithField("component", "clickhouse_manager"),
	}
}

// Start starts the ClickHouse cluster and waits for health
func (m *clickhouseManager) Start(ctx context.Context) error {
	m.log.Info("starting ClickHouse cluster")

	// Start docker-compose
	if err := m.dockerManager.Start(ctx); err != nil {
		return fmt.Errorf("starting docker compose: %w", err)
	}

	// Wait for health checks
	if err := m.waitForHealth(ctx, healthCheckTimeout); err != nil {
		return fmt.Errorf("waiting for health: %w", err)
	}

	m.log.Info("ClickHouse cluster started and healthy")

	return nil
}

// Stop stops the ClickHouse cluster
func (m *clickhouseManager) Stop() error {
	m.log.Info("stopping ClickHouse cluster")

	// Close connection if open
	if m.conn != nil {
		if err := m.conn.Close(); err != nil {
			m.log.WithError(err).Warn("failed to close connection")
		}
		m.conn = nil
	}

	// Stop docker-compose
	if err := m.dockerManager.Stop(); err != nil {
		return fmt.Errorf("stopping docker compose: %w", err)
	}

	m.log.Info("ClickHouse cluster stopped")

	return nil
}

// HealthCheck performs a health check on ClickHouse
func (m *clickhouseManager) HealthCheck(ctx context.Context) error {
	conn, err := m.getConnection()
	if err != nil {
		return fmt.Errorf("getting connection: %w", err)
	}

	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var result int
	if err := conn.QueryRowContext(queryCtx, "SELECT 1").Scan(&result); err != nil {
		return fmt.Errorf("health check query failed: %w", err)
	}

	if result != 1 {
		return fmt.Errorf("unexpected health check result: %d", result)
	}

	return nil
}

// CleanupEphemeralDatabases drops test databases older than maxAge
func (m *clickhouseManager) CleanupEphemeralDatabases(ctx context.Context, maxAge time.Duration) error {
	m.log.WithField("max_age", maxAge).Info("cleaning up ephemeral test databases")

	conn, err := m.getConnection()
	if err != nil {
		return fmt.Errorf("getting connection: %w", err)
	}

	// Query for test databases
	query := `
		SELECT name, metadata_modification_time
		FROM system.databases
		WHERE name LIKE 'test_%'
	`

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("querying databases: %w", err)
	}
	defer rows.Close()

	var dropped int
	now := time.Now()

	for rows.Next() {
		var dbName string
		var modTime time.Time

		if err := rows.Scan(&dbName, &modTime); err != nil {
			m.log.WithError(err).Warn("failed to scan database row")
			continue
		}

		// Check if database is older than maxAge
		age := now.Sub(modTime)
		if maxAge > 0 && age < maxAge {
			continue
		}

		// Drop database
		m.log.WithFields(logrus.Fields{
			"database": dbName,
			"age":      age,
		}).Info("dropping ephemeral test database")

		dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS `%s` ON CLUSTER cluster_2S_1R", dbName)
		if _, err := conn.ExecContext(ctx, dropSQL); err != nil {
			m.log.WithError(err).WithField("database", dbName).Error("failed to drop database")
			continue
		}

		dropped++
	}

	m.log.WithField("dropped", dropped).Info("cleanup complete")

	return nil
}

// waitForHealth waits for ClickHouse to become healthy
func (m *clickhouseManager) waitForHealth(ctx context.Context, timeout time.Duration) error {
	m.log.Debug("waiting for ClickHouse to become healthy")

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := m.HealthCheck(ctx); err == nil {
				m.log.Debug("ClickHouse is healthy")
				return nil
			}

			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for ClickHouse health")
			}

			m.log.Debug("ClickHouse not ready yet, retrying...")
		}
	}
}

// getConnection returns or creates a connection to ClickHouse
func (m *clickhouseManager) getConnection() (*sql.DB, error) {
	if m.conn != nil {
		return m.conn, nil
	}

	conn, err := sql.Open("clickhouse", m.connStr)
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}

	m.conn = conn
	return m.conn, nil
}
