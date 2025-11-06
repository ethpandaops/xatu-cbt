package infra

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// DockerManager manages docker-compose lifecycle
type DockerManager interface {
	Start(ctx context.Context) error
	Stop() error
	Reset() error
	IsRunning(ctx context.Context) (bool, error)
	GetContainerStatus(ctx context.Context, service string) (string, error)
}

type dockerManager struct {
	composeFile string
	projectName string
	log         logrus.FieldLogger
}

const (
	commandTimeout = 2 * time.Minute
)

// NewDockerManager creates a new docker-compose manager
func NewDockerManager(composeFile, projectName string, log logrus.FieldLogger) DockerManager {
	return &dockerManager{
		composeFile: composeFile,
		projectName: projectName,
		log:         log.WithField("component", "docker_manager"),
	}
}

// Start starts docker-compose services
func (m *dockerManager) Start(ctx context.Context) error {
	m.log.WithFields(logrus.Fields{
		"compose_file": m.composeFile,
		"project":      m.projectName,
	}).Debug("starting docker-compose")

	args := []string{
		"compose",
		"-f", m.composeFile,
		"-p", m.projectName,
		"up", "-d",
	}

	if err := m.execCompose(ctx, args...); err != nil {
		return fmt.Errorf("executing docker-compose up: %w", err)
	}

	m.log.Info("docker-compose services started")

	return nil
}

// Stop stops docker-compose services (volumes are preserved)
func (m *dockerManager) Stop() error {
	m.log.WithFields(logrus.Fields{
		"compose_file": m.composeFile,
		"project":      m.projectName,
	}).Debug("stopping docker-compose")

	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()

	args := []string{
		"compose",
		"-f", m.composeFile,
		"-p", m.projectName,
		"down",
		// Note: Volumes are preserved to keep xatu-clickhouse data persistent
		// Use 'infra reset' command to remove volumes
	}

	if err := m.execCompose(ctx, args...); err != nil {
		return fmt.Errorf("executing docker-compose down: %w", err)
	}

	m.log.Info("docker-compose services stopped (volumes preserved)")

	return nil
}

// Reset stops services and removes all volumes
func (m *dockerManager) Reset() error {
	m.log.WithFields(logrus.Fields{
		"compose_file": m.composeFile,
		"project":      m.projectName,
	}).Debug("resetting docker-compose")

	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()

	args := []string{
		"compose",
		"-f", m.composeFile,
		"-p", m.projectName,
		"down",
		"-v", // Remove volumes for complete reset
	}

	if err := m.execCompose(ctx, args...); err != nil {
		return fmt.Errorf("executing docker-compose down: %w", err)
	}

	m.log.Info("docker-compose services stopped and volumes removed")

	return nil
}

// IsRunning checks if containers are running
func (m *dockerManager) IsRunning(ctx context.Context) (bool, error) {
	m.log.Debug("checking if containers are running")

	args := []string{
		"compose",
		"-f", m.composeFile,
		"-p", m.projectName,
		"ps", "-q",
	}

	output, err := m.execComposeOutput(ctx, args...)
	if err != nil {
		return false, fmt.Errorf("executing docker-compose ps: %w", err)
	}

	// If output is not empty, containers are running
	running := strings.TrimSpace(string(output)) != ""

	m.log.WithField("running", running).Debug("container status checked")

	return running, nil
}

// GetContainerStatus returns the status of a specific service
func (m *dockerManager) GetContainerStatus(ctx context.Context, service string) (string, error) {
	m.log.WithField("service", service).Debug("getting container status")

	args := []string{
		"compose",
		"-f", m.composeFile,
		"-p", m.projectName,
		"ps", service,
	}

	output, err := m.execComposeOutput(ctx, args...)
	if err != nil {
		return "", fmt.Errorf("executing docker-compose ps: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) < 2 {
		return "unknown", nil
	}

	// Parse status from output
	// Format: NAME  IMAGE  COMMAND  SERVICE  CREATED  STATUS  PORTS
	statusLine := lines[len(lines)-1]
	fields := strings.Fields(statusLine)
	if len(fields) >= 6 {
		return fields[5], nil
	}

	return "unknown", nil
}

// execCompose executes a docker-compose command
func (m *dockerManager) execCompose(ctx context.Context, args ...string) error {
	execCtx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()

	cmd := exec.CommandContext(execCtx, "docker", args...)
	m.log.WithField("command", cmd.String()).Debug("executing docker command")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("command failed: %w\nOutput: %s", err, string(output))
	}

	return nil
}

// execComposeOutput executes a docker-compose command and returns output
func (m *dockerManager) execComposeOutput(ctx context.Context, args ...string) ([]byte, error) {
	execCtx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()

	cmd := exec.CommandContext(execCtx, "docker", args...)
	m.log.WithField("command", cmd.String()).Debug("executing docker command")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("command failed: %w\nOutput: %s", err, string(output))
	}

	return output, nil
}
