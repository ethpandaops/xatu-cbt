package test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// Common errors for Docker operations
var (
	ErrContainerExitTimeout   = errors.New("timeout waiting for container to exit")
	ErrContainerHealthTimeout = errors.New("timeout waiting for container to be healthy")
	ErrNoCommand              = errors.New("no command provided")
)

// DockerManager handles Docker and Docker Compose operations
type DockerManager interface {
	ComposeDown(ctx context.Context, dir string, removeVolumes bool) error
	ComposeUp(ctx context.Context, dir string, profiles []string) error
	CloneRepository(ctx context.Context, repoURL, targetDir, ref string) error
	WaitForContainerExit(ctx context.Context, containerName string, timeout time.Duration) error
	WaitForContainerHealthy(ctx context.Context, containerName string, timeout time.Duration) error
	RunCommand(ctx context.Context, dir string, command ...string) error
}

type dockerManager struct {
	log logrus.FieldLogger
}

// NewDockerManager creates a new Docker manager instance
func NewDockerManager(log logrus.FieldLogger) DockerManager {
	return &dockerManager{
		log: log.WithField("component", "docker_manager"),
	}
}

func (d *dockerManager) ComposeDown(ctx context.Context, dir string, removeVolumes bool) error {
	args := []string{"compose", "down"}
	if removeVolumes {
		args = append(args, "-v")
	}

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	d.log.WithFields(logrus.Fields{
		"dir":     dir,
		"volumes": removeVolumes,
	}).Debug("Running docker compose down")

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose down failed: %w", err)
	}

	return nil
}

func (d *dockerManager) ComposeUp(ctx context.Context, dir string, profiles []string) error {
	args := []string{"compose"}

	// Add profiles if specified
	for _, profile := range profiles {
		args = append(args, "--profile", profile)
	}

	args = append(args, "up", "-d")

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Pass through environment variables, including NETWORK
	cmd.Env = os.Environ()

	d.log.WithFields(logrus.Fields{
		"dir":      dir,
		"profiles": profiles,
	}).Debug("Running docker compose up")

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose up failed: %w", err)
	}

	return nil
}

func (d *dockerManager) CloneRepository(ctx context.Context, repoURL, targetDir, ref string) error {
	// Remove target directory if it exists
	if err := os.RemoveAll(targetDir); err != nil {
		return fmt.Errorf("failed to remove existing directory: %w", err)
	}

	// Clone the repository
	cmd := exec.CommandContext(ctx, "git", "clone", repoURL, targetDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	d.log.WithFields(logrus.Fields{
		"repo": repoURL,
		"dir":  targetDir,
	}).Debug("Cloning repository")

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git clone failed: %w", err)
	}

	// Checkout the specified ref
	if ref != "" && ref != "master" && ref != "main" {
		checkoutCmd := exec.CommandContext(ctx, "git", "checkout", ref)
		checkoutCmd.Dir = targetDir
		checkoutCmd.Stdout = os.Stdout
		checkoutCmd.Stderr = os.Stderr

		d.log.WithField("ref", ref).Debug("Checking out ref")

		if err := checkoutCmd.Run(); err != nil {
			return fmt.Errorf("git checkout failed: %w", err)
		}
	}

	return nil
}

func (d *dockerManager) WaitForContainerExit(ctx context.Context, containerName string, timeout time.Duration) error {
	d.log.WithField("container", containerName).Info("Waiting for container to exit")

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Counter for periodic status updates
	checkCount := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			checkCount++

			if time.Now().After(deadline) {
				return fmt.Errorf("%w: %s", ErrContainerExitTimeout, containerName)
			}

			// Check if container exists and has exited
			cmd := exec.CommandContext(ctx, "docker", "ps", "-a", "--filter", fmt.Sprintf("name=%s", containerName), "--format", "{{.Status}}") //nolint:gosec // containerName is controlled
			output, err := cmd.Output()
			if err != nil {
				continue // Container might not exist yet
			}

			status := strings.TrimSpace(string(output))
			if status == "" {
				continue // Container doesn't exist yet
			}

			if strings.HasPrefix(status, "Exited") {
				// Get the last log line from the container before it exited
				logCmd := exec.CommandContext(ctx, "docker", "logs", containerName, "--tail", "1")
				if logOutput, err := logCmd.Output(); err == nil && len(logOutput) > 0 {
					lastLog := strings.TrimSpace(string(logOutput))
					if lastLog != "" {
						d.log.WithFields(logrus.Fields{
							"container": containerName,
							"last_log":  lastLog,
						}).Info("Container last log")
					}
				}

				d.log.WithField("container", containerName).Info("Container has exited successfully")
				return nil
			}

			// Provide periodic feedback every 10 seconds (5 checks * 2 seconds)
			if checkCount%5 == 0 {
				// Get the container's current last log line
				logCmd := exec.CommandContext(ctx, "docker", "logs", containerName, "--tail", "1")
				logOutput, err := logCmd.CombinedOutput()

				fields := logrus.Fields{
					"container": containerName,
					"status":    status,
					"elapsed":   time.Since(deadline.Add(-timeout)).Round(time.Second),
				}

				switch {
				case err != nil:
					fields["log_error"] = err.Error()
				case len(logOutput) > 0:
					lastLog := strings.TrimSpace(string(logOutput))
					if lastLog != "" {
						fields["last_log"] = lastLog
					} else {
						fields["last_log"] = "(empty log line)"
					}
				default:
					fields["last_log"] = "(no logs yet)"
				}

				d.log.WithFields(fields).Info("Still waiting for container to finish...")
			}
		}
	}
}

func (d *dockerManager) WaitForContainerHealthy(ctx context.Context, containerName string, timeout time.Duration) error {
	d.log.WithField("container", containerName).Debug("Waiting for container to be healthy")

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("%w: %s", ErrContainerHealthTimeout, containerName)
			}

			// Check container health status
			cmd := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.State.Health.Status}}", containerName)
			output, err := cmd.Output()
			if err != nil {
				// Container might not exist yet or might not have health check
				// Try to check if it's at least running
				runCmd := exec.CommandContext(ctx, "docker", "ps", "--filter", fmt.Sprintf("name=%s", containerName), "--format", "{{.Status}}") //nolint:gosec // containerName is controlled
				runOutput, runErr := runCmd.Output()
				if runErr == nil && strings.Contains(string(runOutput), "Up") {
					// Container is running, might not have health check
					d.log.WithField("container", containerName).Debug("Container is running (no health check)")
					return nil
				}
				continue
			}

			health := strings.TrimSpace(string(output))
			if health == "healthy" {
				d.log.WithField("container", containerName).Debug("Container is healthy")
				return nil
			}

			d.log.WithFields(logrus.Fields{
				"container": containerName,
				"health":    health,
			}).Debug("Container not healthy yet")
		}
	}
}

func (d *dockerManager) RunCommand(ctx context.Context, dir string, command ...string) error {
	if len(command) == 0 {
		return ErrNoCommand
	}

	cmd := exec.CommandContext(ctx, command[0], command[1:]...) //nolint:gosec // command is from controlled source
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	d.log.WithFields(logrus.Fields{
		"dir":     dir,
		"command": strings.Join(command, " "),
	}).Debug("Running command")

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("command failed: %w", err)
	}

	return nil
}
