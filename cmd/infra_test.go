package cmd

import (
	"context"
	"os"
	"testing"

	"github.com/ethpandaops/xatu-cbt/internal/config"
	"github.com/ethpandaops/xatu-cbt/internal/infra"
	"github.com/stretchr/testify/require"
)

func TestConfigureCBTMigrationTargetCustomProjectUsesPublishedPort(t *testing.T) {
	dockerManager := &fakeDockerManager{
		publishedPort: infra.PublishedPort{Host: "127.0.0.1", Port: "9100"},
	}
	t.Setenv("CLICKHOUSE_HOST", "localhost")
	t.Setenv(config.ClickHouseCBT01NativePortEnvVar, "9000")

	restoreEnv, err := configureCBTMigrationTarget(context.Background(), dockerManager, "a")

	require.NoError(t, err)
	require.Equal(t, config.ClickHouseContainer, dockerManager.service)
	require.Equal(t, config.ClickHouseContainerNativePort, dockerManager.targetPort)
	require.Equal(t, "127.0.0.1", getTestEnv(t, "CLICKHOUSE_HOST"))
	require.Equal(t, "9100", getTestEnv(t, config.ClickHouseCBT01NativePortEnvVar))

	restoreEnv()
	require.Equal(t, "localhost", getTestEnv(t, "CLICKHOUSE_HOST"))
	require.Equal(t, "9000", getTestEnv(t, config.ClickHouseCBT01NativePortEnvVar))
}

func TestResolveXatuMigrationConnStrCustomProjectUsesPublishedPort(t *testing.T) {
	dockerManager := &fakeDockerManager{
		publishedPort: infra.PublishedPort{Host: "127.0.0.1", Port: "9202"},
	}
	t.Setenv("CLICKHOUSE_USERNAME", "")
	t.Setenv("CLICKHOUSE_PASSWORD", "")

	connStr, err := resolveXatuMigrationConnStr(context.Background(), dockerManager, "a")

	require.NoError(t, err)
	require.Equal(t, xatuClickHouseService, dockerManager.service)
	require.Equal(t, config.ClickHouseContainerNativePort, dockerManager.targetPort)
	require.Equal(t, []string{"xatu-local"}, dockerManager.profiles)
	require.Equal(t, "clickhouse://default:supersecret@127.0.0.1:9202", connStr)
}

func TestResolveXatuMigrationConnStrDefaultProjectUsesPublishedPort(t *testing.T) {
	dockerManager := &fakeDockerManager{
		publishedPort: infra.PublishedPort{Host: "127.0.0.1", Port: "9202"},
	}
	t.Setenv("CLICKHOUSE_HOST", "")
	t.Setenv("CLICKHOUSE_USERNAME", "")
	t.Setenv("CLICKHOUSE_PASSWORD", "")
	t.Setenv(config.ClickHouseNativePortEnvVar, "")
	t.Setenv(config.ClickHouseXatu01NativePortEnvVar, "9302")

	connStr, err := resolveXatuMigrationConnStr(context.Background(), dockerManager, config.ProjectName)

	require.NoError(t, err)
	require.Equal(t, xatuClickHouseService, dockerManager.service)
	require.Equal(t, config.ClickHouseContainerNativePort, dockerManager.targetPort)
	require.Equal(t, []string{"xatu-local"}, dockerManager.profiles)
	require.Equal(t, "clickhouse://default:supersecret@127.0.0.1:9202", connStr)
}

type fakeDockerManager struct {
	publishedPort infra.PublishedPort
	service       string
	targetPort    string
	profiles      []string
}

func (m *fakeDockerManager) Start(context.Context, ...string) error {
	return nil
}

func (m *fakeDockerManager) Stop(...string) error {
	return nil
}

func (m *fakeDockerManager) Reset(...string) error {
	return nil
}

func (m *fakeDockerManager) IsRunning(context.Context) (bool, error) {
	return false, nil
}

func (m *fakeDockerManager) GetContainerStatus(context.Context, string) (string, error) {
	return "", nil
}

func (m *fakeDockerManager) GetAllServices(context.Context) ([]infra.ServiceInfo, error) {
	return nil, nil
}

func (m *fakeDockerManager) GetServicePort(_ context.Context, service, targetPort string, profiles ...string) (infra.PublishedPort, error) {
	m.service = service
	m.targetPort = targetPort
	m.profiles = profiles

	return m.publishedPort, nil
}

func getTestEnv(t *testing.T, key string) string {
	t.Helper()

	value, ok := os.LookupEnv(key)
	require.True(t, ok)

	return value
}
