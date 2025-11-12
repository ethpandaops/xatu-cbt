package infra

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	commandTimeout = 2 * time.Minute
)

// ServiceInfo holds information about a running service.
type ServiceInfo struct {
	Name   string
	Status string
	Ports  string
}

// DockerManager manages docker-compose lifecycle.
type DockerManager interface {
	Start(ctx context.Context, profiles ...string) error
	Stop(profiles ...string) error
	Reset(profiles ...string) error
	IsRunning(ctx context.Context) (bool, error)
	GetContainerStatus(ctx context.Context, service string) (string, error)
	GetAllServices(ctx context.Context) ([]ServiceInfo, error)
}

type dockerManager struct {
	composeFile string
	projectName string
	log         logrus.FieldLogger
}

// NewDockerManager creates a new docker-compose manager.
func NewDockerManager(log logrus.FieldLogger, composeFile, projectName string) DockerManager {
	return &dockerManager{
		composeFile: composeFile,
		projectName: projectName,
		log:         log.WithField("component", "docker_manager"),
	}
}

// Start starts docker-compose services.
func (m *dockerManager) Start(ctx context.Context, profiles ...string) error {
	m.log.WithFields(logrus.Fields{
		"compose_file": m.composeFile,
		"project":      m.projectName,
		"profiles":     profiles,
	}).Debug("starting docker-compose")

	args := []string{
		"compose",
		"-f", m.composeFile,
		"-p", m.projectName,
	}

	// Add profile flags if specified
	for _, profile := range profiles {
		args = append(args, "--profile", profile)
	}

	args = append(args, "up", "-d", "--wait")

	if _, err := m.execComposeOutput(ctx, args...); err != nil {
		return fmt.Errorf("executing docker-compose up: %w", err)
	}

	m.log.Info("docker-compose services started")

	return nil
}

// Stop stops docker-compose services (volumes are preserved).
// Profiles should be passed to ensure all containers are stopped, including those in profiles.
func (m *dockerManager) Stop(profiles ...string) error {
	m.log.WithFields(logrus.Fields{
		"compose_file": m.composeFile,
		"project":      m.projectName,
		"profiles":     profiles,
	}).Debug("stopping docker-compose")

	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()

	args := []string{
		"compose",
		"-f", m.composeFile,
		"-p", m.projectName,
	}

	// Add specified profiles to ensure all containers are stopped
	for _, profile := range profiles {
		args = append(args, "--profile", profile)
	}

	args = append(args, "down")

	if _, err := m.execComposeOutput(ctx, args...); err != nil {
		return fmt.Errorf("executing docker-compose down: %w", err)
	}

	m.log.Info("docker-compose services stopped")

	return nil
}

// Reset stops services and removes all volumes.
// Profiles should be passed to ensure all containers are removed, including those in profiles.
func (m *dockerManager) Reset(profiles ...string) error {
	m.log.WithFields(logrus.Fields{
		"compose_file": m.composeFile,
		"project":      m.projectName,
		"profiles":     profiles,
	}).Debug("resetting docker-compose")

	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()

	args := []string{
		"compose",
		"-f", m.composeFile,
		"-p", m.projectName,
	}

	// Add specified profiles to ensure all containers are removed
	for _, profile := range profiles {
		args = append(args, "--profile", profile)
	}

	args = append(args, "down", "-v")

	if _, err := m.execComposeOutput(ctx, args...); err != nil {
		return fmt.Errorf("executing docker-compose down: %w", err)
	}

	m.log.Info("docker-compose services stopped and volumes removed")

	return nil
}

// IsRunning checks if containers are running.
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

	running := strings.TrimSpace(string(output)) != ""

	m.log.WithField("running", running).Debug("container status checked")

	return running, nil
}

// GetContainerStatus returns the status of a specific service.
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

	statusLine := lines[len(lines)-1]
	fields := strings.Fields(statusLine)
	if len(fields) >= 6 {
		return fields[5], nil
	}

	return "unknown", nil
}

// GetAllServices returns detailed information about all running services.
func (m *dockerManager) GetAllServices(ctx context.Context) ([]ServiceInfo, error) { //nolint:gocyclo // Complex Docker service parsing - refactoring would risk breaking Docker integration
	m.log.Debug("getting all services")

	args := []string{
		"compose",
		"-f", m.composeFile,
		"-p", m.projectName,
		"ps", "--format", "json",
	}

	output, err := m.execComposeOutput(ctx, args...)
	if err != nil {
		return nil, fmt.Errorf("executing docker-compose ps: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	services := make([]ServiceInfo, 0, len(lines))

	for _, line := range lines {
		if line == "" {
			continue
		}

		var data map[string]any
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			m.log.WithError(err).Debug("failed to parse service json")

			continue
		}

		var (
			name   string
			status string
			ports  string
		)

		if n, ok := data["Service"].(string); ok {
			name = n
		}

		if s, ok := data["State"].(string); ok {
			status = s
			if h, ok := data["Health"].(string); ok && h != "" {
				status = fmt.Sprintf("%s (%s)", s, h)
			}
		}

		//nolint:nestif // Complex port parsing logic.
		if p, ok := data["Publishers"].([]any); ok && len(p) > 0 {
			portStrs := make([]string, 0, len(p))

			for _, pub := range p {
				pubMap, ok := pub.(map[string]any)
				if !ok {
					continue
				}

				var (
					publishedPort string
					targetPort    string
				)

				if pp, ok := pubMap["PublishedPort"].(float64); ok {
					publishedPort = fmt.Sprintf("%.0f", pp)
				}

				if tp, ok := pubMap["TargetPort"].(float64); ok {
					targetPort = fmt.Sprintf("%.0f", tp)
				}

				if publishedPort != "" && targetPort != "" {
					portStrs = append(portStrs, fmt.Sprintf("%s->%s", publishedPort, targetPort))
				}
			}

			if len(portStrs) > 0 {
				ports = strings.Join(portStrs, ", ")
			}
		}

		if name != "" {
			services = append(services, ServiceInfo{
				Name:   name,
				Status: status,
				Ports:  ports,
			})
		}
	}

	return services, nil
}

// execComposeOutput executes a docker-compose command and returns output.
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
