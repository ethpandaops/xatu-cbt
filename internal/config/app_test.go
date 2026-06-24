package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadUsesCBTNativePortEnv(t *testing.T) {
	clearConnectionEnv(t)
	t.Chdir(t.TempDir())
	t.Setenv(ClickHouseNativePortEnvVar, "9000")
	t.Setenv(ClickHouseCBT01NativePortEnvVar, "9100")

	cfg, err := Load()

	require.NoError(t, err)
	require.Equal(t, 9100, cfg.ClickhouseNativePort)
}

func TestLoadDefaultsCBTNativePort(t *testing.T) {
	clearConnectionEnv(t)
	t.Chdir(t.TempDir())

	cfg, err := Load()

	require.NoError(t, err)
	require.Equal(t, 9000, cfg.ClickhouseNativePort)
}

func TestGetCBTClickHouseURLUsesCBTNativePortEnv(t *testing.T) {
	clearConnectionEnv(t)
	t.Setenv(ClickHouseNativePortEnvVar, "9000")
	t.Setenv(ClickHouseCBT01NativePortEnvVar, "9100")

	require.Equal(t, "clickhouse://default:supersecret@localhost:9100", GetCBTClickHouseURL())
}

func TestGetXatuClickHouseURLUsesXatuNativePortEnv(t *testing.T) {
	clearConnectionEnv(t)
	t.Setenv(ClickHouseNativePortEnvVar, "9000")
	t.Setenv(ClickHouseXatu01NativePortEnvVar, "9202")

	require.Equal(t, "clickhouse://default:supersecret@localhost:9202", GetXatuClickHouseURL())
}

func TestGetXatuClickHouseURLDefaultsXatuNativePort(t *testing.T) {
	clearConnectionEnv(t)

	require.Equal(t, "clickhouse://default:supersecret@localhost:9002", GetXatuClickHouseURL())
}

func TestGetXatuClickHouseURLIgnoresLegacyNativePort(t *testing.T) {
	clearConnectionEnv(t)
	t.Setenv(ClickHouseNativePortEnvVar, "9000")

	require.Equal(t, "clickhouse://default:supersecret@localhost:9002", GetXatuClickHouseURL())
}

func TestGetRedisURLUsesRedisPortEnv(t *testing.T) {
	clearConnectionEnv(t)
	t.Setenv(RedisPortEnvVar, "7380")

	require.Equal(t, "redis://localhost:7380", GetRedisURL())
}

func TestGetRedisURLDefaultsRedisPort(t *testing.T) {
	clearConnectionEnv(t)

	require.Equal(t, DefaultRedisURL, GetRedisURL())
}

func TestGetDockerNetworkNameUsesProjectName(t *testing.T) {
	clearConnectionEnv(t)
	t.Setenv(ProjectNameEnvVar, "a")

	require.Equal(t, "a_xatu-net", GetDockerNetworkName())
}

func TestGetDockerNetworkNameDefaultsProjectName(t *testing.T) {
	clearConnectionEnv(t)
	t.Setenv(ProjectNameEnvVar, "")

	require.Equal(t, "xatu-cbt-platform_xatu-net", GetDockerNetworkName())
}

func clearConnectionEnv(t *testing.T) {
	t.Helper()

	for _, key := range []string{
		"CLICKHOUSE_HOST",
		"CLICKHOUSE_USERNAME",
		"CLICKHOUSE_PASSWORD",
		ClickHouseNativePortEnvVar,
		ClickHouseCBT01NativePortEnvVar,
		ClickHouseXatu01NativePortEnvVar,
		RedisPortEnvVar,
		ProjectNameEnvVar,
	} {
		t.Setenv(key, "")
	}
}
