// Package infra provides infrastructure management for Docker and ClickHouse clusters.
package infra

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // ClickHouse driver registration
	"github.com/sirupsen/logrus"
)

const (
	healthCheckTimeout  = 120 * time.Second
	healthCheckInterval = 5 * time.Second
)

var (
	errClickHouseHealthTimeout = errors.New("timeout waiting for ClickHouse health")
)

// ClickHouseManager manages ClickHouse cluster lifecycle
type ClickHouseManager interface {
	Start(ctx context.Context, profiles ...string) error
	Stop(profiles ...string) error
	HealthCheck(ctx context.Context) error
	CleanupEphemeralDatabases(ctx context.Context, maxAge time.Duration) error
}

type clickhouseManager struct {
	dockerManager DockerManager
	connStr       string
	log           logrus.FieldLogger
	validator     Validator
	validated     bool
	conn          *sql.DB
}

// NewClickHouseManager creates a new ClickHouse cluster manager.
func NewClickHouseManager(log logrus.FieldLogger, dockerManager DockerManager, connStr string, safeHostnames []string) ClickHouseManager {
	return &clickhouseManager{
		dockerManager: dockerManager,
		connStr:       connStr,
		log:           log.WithField("component", "clickhouse_manager"),
		validator:     NewValidator(safeHostnames, log),
		validated:     false,
	}
}

// Start starts the ClickHouse cluster and waits for health.
func (m *clickhouseManager) Start(ctx context.Context, profiles ...string) error {
	m.log.Info("starting clickhouse cluster")

	if err := m.dockerManager.Start(ctx, profiles...); err != nil {
		return fmt.Errorf("starting docker compose: %w", err)
	}

	if err := m.waitForHealth(ctx, healthCheckTimeout); err != nil {
		return fmt.Errorf("waiting for health: %w", err)
	}

	m.log.Info("clickhouse cluster started and healthy")

	return nil
}

// Stop stops the ClickHouse cluster.
// Profiles should be passed to ensure all containers are stopped, including those in profiles.
func (m *clickhouseManager) Stop(profiles ...string) error {
	m.log.WithField("profiles", profiles).Info("stopping clickhouse cluster")

	if m.conn != nil {
		if err := m.conn.Close(); err != nil {
			m.log.WithError(err).Warn("failed to close connection")
		}
		m.conn = nil
	}

	if err := m.dockerManager.Stop(profiles...); err != nil {
		return fmt.Errorf("stopping docker compose: %w", err)
	}

	m.log.Info("clickhouse cluster stopped")

	return nil
}

// HealthCheck performs a health check on ClickHouse.
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
		return fmt.Errorf("unexpected health check result: %d", result) //nolint:err113 // Include actual result value for debugging
	}

	return nil
}

// CleanupEphemeralDatabases drops test databases.
// If maxAge is 0, all test databases are dropped. Otherwise, only databases
// older than maxAge are dropped (based on creation time if available).
func (m *clickhouseManager) CleanupEphemeralDatabases(ctx context.Context, maxAge time.Duration) error {
	m.log.WithField("max_age", maxAge).Info("cleaning up ephemeral databases")

	conn, err := m.getConnection()
	if err != nil {
		return fmt.Errorf("getting connection: %w", err)
	}

	// Query for test databases - only select name since metadata columns vary by ClickHouse version
	query := `
		SELECT name
		FROM system.databases
		WHERE name LIKE 'test_%' OR name LIKE 'cbt_%'
	`

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("querying databases: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			m.log.WithError(err).Debug("error closing rows")
		}
	}()

	var dropped int

	for rows.Next() {
		var dbName string

		if err := rows.Scan(&dbName); err != nil {
			m.log.WithError(err).Warn("failed to scan database row")
			continue
		}

		m.log.WithField("database", dbName).Info("dropping ephemeral test database")

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

// waitForHealth waits for ClickHouse to become healthy.
func (m *clickhouseManager) waitForHealth(ctx context.Context, timeout time.Duration) error {
	m.log.Debug("waiting for clickhouse to become healthy")

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := m.HealthCheck(ctx); err == nil {
				m.log.Debug("clickhouse is healthy")
				return nil
			}

			if time.Now().After(deadline) {
				return errClickHouseHealthTimeout
			}

			m.log.Debug("clickhouse not ready yet, retrying...")
		}
	}
}

// getConnection returns or creates a connection to ClickHouse.
func (m *clickhouseManager) getConnection() (*sql.DB, error) {
	if m.conn != nil {
		return m.conn, nil
	}

	conn, err := sql.Open("clickhouse", m.connStr)
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}

	// Validate hostname on first connection
	if !m.validated {
		m.log.Debug("validating ClickHouse hostname against whitelist")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := m.validator.Validate(ctx, conn); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("hostname validation failed: %w", err)
		}
		m.validated = true
	}

	m.conn = conn
	return m.conn, nil
}
