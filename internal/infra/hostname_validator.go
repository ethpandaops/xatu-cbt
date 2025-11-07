package infra

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/sirupsen/logrus"
)

var (
	// ErrConnectionNil is returned when the database connection is nil.
	ErrConnectionNil = errors.New("database connection is nil")

	// ErrNonWhitelistedHost is returned when attempting to connect to a non-whitelisted ClickHouse host.
	ErrNonWhitelistedHost = errors.New("refusing to connect to non-whitelisted ClickHouse host")
)

// Validator validates ClickHouse hostnames against a whitelist to prevent
// accidental destructive operations on production databases.
type Validator interface {
	// Validate checks if the connected ClickHouse hostname is in the whitelist.
	// Returns an error if the hostname is not whitelisted or if the check fails.
	Validate(ctx context.Context, conn *sql.DB) error

	// ValidateMultiple checks multiple database connections.
	// Returns an error if any hostname is not whitelisted.
	ValidateMultiple(ctx context.Context, conns ...*sql.DB) error

	// ValidateDriver validates a ClickHouse native driver connection.
	// Returns an error if the hostname is not whitelisted or if the check fails.
	ValidateDriver(ctx context.Context, conn driver.Conn) error
}

type validator struct {
	safeHostnames []string
	log           logrus.FieldLogger
}

// Compile-time check to ensure validator implements Validator interface.
var _ Validator = (*validator)(nil)

// NewValidator creates a new hostname validator with the provided whitelist.
// The validator will block any destructive operations on ClickHouse instances
// whose hostname is not in the safeHostnames whitelist.
func NewValidator(safeHostnames []string, log logrus.FieldLogger) Validator {
	return &validator{
		safeHostnames: safeHostnames,
		log:           log.WithField("component", "hostname_validator"),
	}
}

// Validate checks if the connected ClickHouse hostname is whitelisted.
func (v *validator) Validate(ctx context.Context, conn *sql.DB) error {
	if conn == nil {
		return ErrConnectionNil
	}

	// Query ClickHouse for its hostname
	var hostname string
	query := "SELECT hostName()"

	row := conn.QueryRowContext(ctx, query)
	if err := row.Scan(&hostname); err != nil {
		return fmt.Errorf("failed to query ClickHouse hostname: %w", err)
	}

	// Check if hostname is in the whitelist
	hostname = strings.TrimSpace(hostname)
	if !v.isWhitelisted(hostname) {
		return v.newSafetyError(hostname)
	}

	v.log.WithFields(logrus.Fields{
		"hostname":  hostname,
		"whitelist": v.safeHostnames,
	}).Info("ClickHouse hostname validated successfully")

	return nil
}

// ValidateMultiple validates multiple database connections.
func (v *validator) ValidateMultiple(ctx context.Context, conns ...*sql.DB) error {
	for i, conn := range conns {
		if err := v.Validate(ctx, conn); err != nil {
			return fmt.Errorf("validation failed for connection %d: %w", i, err)
		}
	}
	return nil
}

// ValidateDriver validates a ClickHouse native driver connection.
func (v *validator) ValidateDriver(ctx context.Context, conn driver.Conn) error {
	if conn == nil {
		return ErrConnectionNil
	}

	// Query ClickHouse for its hostname using native driver
	var hostname string
	query := "SELECT hostName()"

	row := conn.QueryRow(ctx, query)
	if err := row.Scan(&hostname); err != nil {
		return fmt.Errorf("failed to query ClickHouse hostname: %w", err)
	}

	// Check if hostname is in the whitelist
	hostname = strings.TrimSpace(hostname)
	if !v.isWhitelisted(hostname) {
		return v.newSafetyError(hostname)
	}

	v.log.WithFields(logrus.Fields{
		"hostname":  hostname,
		"whitelist": v.safeHostnames,
	}).Info("ClickHouse hostname validated successfully (native driver)")

	return nil
}

// isWhitelisted checks if a hostname is in the safe hostnames list.
func (v *validator) isWhitelisted(hostname string) bool {
	for _, safe := range v.safeHostnames {
		if hostname == safe {
			return true
		}
	}
	return false
}

// newSafetyError creates a formatted safety error for non-whitelisted hosts.
func (v *validator) newSafetyError(hostname string) error {
	return fmt.Errorf(
		"SAFETY: refusing to connect to non-whitelisted ClickHouse host '%s'. "+
			"This protection prevents accidental truncation of production databases. "+
			"To allow operations on this host, add it to the SafeHostnames list in your test configuration. "+
			"Current whitelist: %v: %w",
		hostname,
		v.safeHostnames,
		ErrNonWhitelistedHost,
	)
}
