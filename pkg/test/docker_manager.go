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
				d.log.WithField("container", containerName).Info("Container has exited successfully")
				return nil
			}

			// Provide periodic feedback every 10 seconds (5 checks * 2 seconds)
			if checkCount%5 == 0 {
				d.log.WithFields(logrus.Fields{
					"container": containerName,
					"status":    status,
					"elapsed":   time.Since(deadline.Add(-timeout)).Round(time.Second),
				}).Info("Still waiting for container to finish...")

				// Debug the migrator container periodically
				if containerName == "xatu-clickhouse-migrator" && (checkCount == 10 || checkCount == 30) {
					d.debugMigratorContainer(ctx)
				}
			}
		}
	}
}

func (d *dockerManager) debugMigratorContainer(ctx context.Context) {
	d.log.Info("=== COMPREHENSIVE MIGRATOR DEBUG START ===")

	// Use a short timeout context for each command
	makeTimeoutContext := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(ctx, 3*time.Second)
	}

	// 1. Container State
	func() {
		debugCtx, cancel := makeTimeoutContext()
		defer cancel()
		stateCmd := exec.CommandContext(debugCtx, "docker", "inspect", "xatu-clickhouse-migrator",
			"--format", "Status={{.State.Status}} Running={{.State.Running}} ExitCode={{.State.ExitCode}} Pid={{.State.Pid}} StartedAt={{.State.StartedAt}}")
		if out, err := stateCmd.Output(); err == nil {
			d.log.WithField("state", strings.TrimSpace(string(out))).Info("[DEBUG] Container state")
		} else {
			d.log.WithError(err).Error("[DEBUG] Failed to get container state")
		}
	}()

	// 2. Container Command Configuration
	func() {
		debugCtx, cancel := makeTimeoutContext()
		defer cancel()
		cmdCmd := exec.CommandContext(debugCtx, "docker", "inspect", "xatu-clickhouse-migrator",
			"--format", "Cmd={{.Config.Cmd}} Entrypoint={{.Config.Entrypoint}} WorkingDir={{.Config.WorkingDir}}")
		if out, err := cmdCmd.Output(); err == nil {
			d.log.WithField("command", strings.TrimSpace(string(out))).Info("[DEBUG] Container command")
		}
	}()

	// 3. Container Environment
	func() {
		debugCtx, cancel := makeTimeoutContext()
		defer cancel()
		envCmd := exec.CommandContext(debugCtx, "docker", "inspect", "xatu-clickhouse-migrator",
			"--format", "{{range $index, $value := .Config.Env}}{{if $index}}, {{end}}{{$value}}{{end}}")
		if out, err := envCmd.Output(); err == nil {
			d.log.WithField("env", strings.TrimSpace(string(out))).Info("[DEBUG] Container environment")
		}
	}()

	// 4. Container Logs (multiple attempts)
	func() {
		debugCtx, cancel := makeTimeoutContext()
		defer cancel()

		// Try getting all logs
		logCmd := exec.CommandContext(debugCtx, "docker", "logs", "xatu-clickhouse-migrator")
		logOutput, logErr := logCmd.CombinedOutput()

		if logErr != nil {
			if strings.Contains(logErr.Error(), "context deadline") {
				d.log.Error("[DEBUG] Docker logs command timed out - container may be hanging")
			} else {
				d.log.WithError(logErr).Error("[DEBUG] Failed to get container logs")
			}
		} else if len(logOutput) > 0 {
			// Truncate if too long
			if len(logOutput) > 5000 {
				d.log.WithField("logs_truncated", string(logOutput[:5000])+"...[truncated]").Info("[DEBUG] Container logs")
			} else {
				d.log.WithField("logs", string(logOutput)).Info("[DEBUG] Container logs")
			}
		} else {
			d.log.Error("[DEBUG] Container logs are completely empty")
		}
	}()

	// 5. Container Processes
	func() {
		debugCtx, cancel := makeTimeoutContext()
		defer cancel()
		topCmd := exec.CommandContext(debugCtx, "docker", "top", "xatu-clickhouse-migrator")
		if out, err := topCmd.Output(); err != nil {
			d.log.WithError(err).Error("[DEBUG] Cannot get container processes")
		} else {
			d.log.WithField("processes", string(out)).Info("[DEBUG] Container processes")
		}
	}()

	// 6. Test container responsiveness
	func() {
		debugCtx, cancel := makeTimeoutContext()
		defer cancel()
		execCmd := exec.CommandContext(debugCtx, "docker", "exec", "xatu-clickhouse-migrator", "echo", "responsive")
		if _, err := execCmd.CombinedOutput(); err != nil {
			if strings.Contains(err.Error(), "context deadline") {
				d.log.Error("[DEBUG] Container exec timed out - container is unresponsive/hanging")
			} else {
				d.log.WithError(err).Error("[DEBUG] Container exec failed")
			}
		} else {
			d.log.Info("[DEBUG] Container is responsive to exec")
		}
	}()

	// 7. Check ClickHouse accessibility
	func() {
		debugCtx, cancel := makeTimeoutContext()
		defer cancel()
		chCmd := exec.CommandContext(debugCtx, "docker", "exec", "xatu-clickhouse-01",
			"clickhouse-client", "-q", "SELECT version()")
		if out, err := chCmd.Output(); err != nil {
			d.log.WithError(err).Error("[DEBUG] ClickHouse not accessible")
		} else {
			d.log.WithField("version", strings.TrimSpace(string(out))).Info("[DEBUG] ClickHouse is accessible")
		}
	}()

	// 8. Container Resource Usage
	func() {
		debugCtx, cancel := makeTimeoutContext()
		defer cancel()
		statsCmd := exec.CommandContext(debugCtx, "docker", "stats", "--no-stream", "--format",
			"CPU={{.CPUPerc}} MEM={{.MemUsage}} NET={{.NetIO}} BLOCK={{.BlockIO}}", "xatu-clickhouse-migrator")
		if out, err := statsCmd.Output(); err == nil {
			d.log.WithField("stats", strings.TrimSpace(string(out))).Info("[DEBUG] Container resources")
		}
	}()

	// 9. Docker Events (check if container is restarting)
	func() {
		debugCtx, cancel := makeTimeoutContext()
		defer cancel()
		eventsCmd := exec.CommandContext(debugCtx, "docker", "events", "--since", "5m", "--until", "0s",
			"--filter", "container=xatu-clickhouse-migrator", "--format", "{{.Time}} {{.Action}}")
		if out, err := eventsCmd.Output(); err == nil && len(out) > 0 {
			d.log.WithField("events", string(out)).Info("[DEBUG] Recent container events")
		}
	}()

	// 10. Check compose file for migrator configuration
	func() {
		debugCtx, cancel := makeTimeoutContext()
		defer cancel()
		composeCmd := exec.CommandContext(debugCtx, "docker", "compose", "-f", "xatu/docker-compose.yml",
			"config", "--services")
		if out, err := composeCmd.Output(); err == nil {
			d.log.WithField("services", strings.TrimSpace(string(out))).Debug("[DEBUG] Compose services")
		}
	}()

	// 11. Network connectivity check
	func() {
		debugCtx, cancel := makeTimeoutContext()
		defer cancel()
		netCmd := exec.CommandContext(debugCtx, "docker", "inspect", "xatu-clickhouse-migrator",
			"--format", "{{range .NetworkSettings.Networks}}{{.NetworkID}} {{.IPAddress}} {{end}}")
		if out, err := netCmd.Output(); err == nil {
			d.log.WithField("network", strings.TrimSpace(string(out))).Info("[DEBUG] Container network")
		}
	}()

	// 12. Check if migration-related tables exist
	func() {
		debugCtx, cancel := makeTimeoutContext()
		defer cancel()
		tablesCmd := exec.CommandContext(debugCtx, "docker", "exec", "xatu-clickhouse-01",
			"clickhouse-client", "-q", "SELECT count(*) FROM system.tables WHERE database = 'default'")
		if out, err := tablesCmd.Output(); err == nil {
			d.log.WithField("table_count", strings.TrimSpace(string(out))).Info("[DEBUG] Tables in default database")
		}
	}()

	d.log.Info("=== COMPREHENSIVE MIGRATOR DEBUG END ===")
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
