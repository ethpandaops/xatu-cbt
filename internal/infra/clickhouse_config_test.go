package infra

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestLogger() logrus.FieldLogger {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(os.Stderr)

	return log
}

func TestDetectClusterTopology(t *testing.T) {
	log := newTestLogger()

	tests := []struct {
		name           string
		hostname       string
		expectTopology bool
		expectedShards int
	}{
		{
			name:           "known production cluster",
			hostname:       "chendpoint-xatu-clickhouse.analytics.production.ethpandaops",
			expectTopology: true,
			expectedShards: 3,
		},
		{
			name:           "known cluster with different domain",
			hostname:       "chendpoint-xatu-clickhouse.some.other.domain",
			expectTopology: true,
			expectedShards: 3,
		},
		{
			name:           "unknown cluster prefix",
			hostname:       "some-other-clickhouse.analytics.production.ethpandaops",
			expectTopology: false,
			expectedShards: 0,
		},
		{
			name:           "no domain suffix",
			hostname:       "chendpoint-xatu-clickhouse",
			expectTopology: false,
			expectedShards: 0,
		},
		{
			name:           "localhost",
			hostname:       "localhost",
			expectTopology: false,
			expectedShards: 0,
		},
		{
			name:           "IP address",
			hostname:       "192.168.1.1",
			expectTopology: false,
			expectedShards: 0,
		},
		{
			name:           "empty hostname",
			hostname:       "",
			expectTopology: false,
			expectedShards: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topology := detectClusterTopology(log, tt.hostname)

			if tt.expectTopology {
				require.NotNil(t, topology, "expected topology to be detected")
				assert.Equal(t, tt.expectedShards, len(topology.Shards), "unexpected number of shards")

				// Verify each shard has 2 replicas
				for i, shard := range topology.Shards {
					assert.Equal(t, 2, len(shard.Replicas), "shard %d should have 2 replicas", i)
				}
			} else {
				assert.Nil(t, topology, "expected no topology to be detected")
			}
		})
	}
}

func TestDetectClusterTopology_ExpandedHostnames(t *testing.T) {
	log := newTestLogger()

	topology := detectClusterTopology(log, "chendpoint-xatu-clickhouse.analytics.production.ethpandaops")
	require.NotNil(t, topology)

	expectedHosts := [][]string{
		{
			"chendpoint-xatu-clickhouse-0-0.analytics.production.ethpandaops",
			"chendpoint-xatu-clickhouse-0-1.analytics.production.ethpandaops",
		},
		{
			"chendpoint-xatu-clickhouse-1-0.analytics.production.ethpandaops",
			"chendpoint-xatu-clickhouse-1-1.analytics.production.ethpandaops",
		},
		{
			"chendpoint-xatu-clickhouse-2-0.analytics.production.ethpandaops",
			"chendpoint-xatu-clickhouse-2-1.analytics.production.ethpandaops",
		},
	}

	require.Equal(t, len(expectedHosts), len(topology.Shards))

	for i, shard := range topology.Shards {
		require.Equal(t, len(expectedHosts[i]), len(shard.Replicas), "shard %d replica count mismatch", i)
		for j, replica := range shard.Replicas {
			assert.Equal(t, expectedHosts[i][j], replica, "shard %d replica %d hostname mismatch", i, j)
		}
	}
}

func TestGenerateMultiShardXML(t *testing.T) {
	topology := &ExpandedTopology{
		Shards: []ExpandedShard{
			{Replicas: []string{"host-0-0.example.com", "host-0-1.example.com"}},
			{Replicas: []string{"host-1-0.example.com", "host-1-1.example.com"}},
		},
	}

	tests := []struct {
		name     string
		nodeNum  int
		port     int
		username string
		password string
		secure   bool
		checks   []string
	}{
		{
			name:     "basic config",
			nodeNum:  1,
			port:     9000,
			username: "",
			password: "",
			secure:   false,
			checks: []string{
				"<display_name>cluster_2S_1R node 1</display_name>",
				"<host>host-0-0.example.com</host>",
				"<host>host-0-1.example.com</host>",
				"<host>host-1-0.example.com</host>",
				"<host>host-1-1.example.com</host>",
				"<port>9000</port>",
				"<secure>0</secure>",
				"<replica>01</replica>",
			},
		},
		{
			name:     "with credentials",
			nodeNum:  2,
			port:     9440,
			username: "myuser",
			password: "mypass",
			secure:   true,
			checks: []string{
				"<display_name>cluster_2S_1R node 2</display_name>",
				"<port>9440</port>",
				"<user>myuser</user>",
				"<password>mypass</password>",
				"<secure>1</secure>",
				"<replica>02</replica>",
			},
		},
		{
			name:     "username only",
			nodeNum:  1,
			port:     9000,
			username: "readonly",
			password: "",
			secure:   false,
			checks: []string{
				"<user>readonly</user>",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			xml, err := generateMultiShardXML(tt.nodeNum, topology, tt.port, tt.username, tt.password, tt.secure)
			require.NoError(t, err)

			for _, check := range tt.checks {
				assert.Contains(t, xml, check, "XML should contain: %s", check)
			}

			// Verify no credentials appear when not provided
			if tt.username == "" {
				assert.NotContains(t, xml, "<user>")
			}
			if tt.password == "" {
				assert.NotContains(t, xml, "<password>")
			}
		})
	}
}

func TestGenerateMultiShardXML_ShardStructure(t *testing.T) {
	topology := &ExpandedTopology{
		Shards: []ExpandedShard{
			{Replicas: []string{"shard0-replica0.test", "shard0-replica1.test"}},
			{Replicas: []string{"shard1-replica0.test", "shard1-replica1.test"}},
			{Replicas: []string{"shard2-replica0.test", "shard2-replica1.test"}},
		},
	}

	xml, err := generateMultiShardXML(1, topology, 9000, "", "", false)
	require.NoError(t, err)

	// Count shard elements in xatu_cluster section
	xatuClusterStart := strings.Index(xml, "<xatu_cluster>")
	xatuClusterEnd := strings.Index(xml, "</xatu_cluster>")
	require.Greater(t, xatuClusterStart, 0)
	require.Greater(t, xatuClusterEnd, xatuClusterStart)

	xatuClusterSection := xml[xatuClusterStart:xatuClusterEnd]
	shardCount := strings.Count(xatuClusterSection, "<shard>")
	assert.Equal(t, 3, shardCount, "should have 3 shards in xatu_cluster")

	// Each shard should have internal_replication=true
	assert.Equal(t, 3, strings.Count(xatuClusterSection, "<internal_replication>true</internal_replication>"))
}

func TestGenerateSingleShardConfig(t *testing.T) {
	tests := []struct {
		name     string
		nodeNum  int
		host     string
		port     int
		username string
		password string
		secure   bool
		checks   []string
	}{
		{
			name:     "basic config",
			nodeNum:  1,
			host:     "clickhouse.example.com",
			port:     9000,
			username: "",
			password: "",
			secure:   false,
			checks: []string{
				"<display_name>cluster_2S_1R node 1</display_name>",
				"<host>clickhouse.example.com</host>",
				"<port>9000</port>",
				"<secure>0</secure>",
			},
		},
		{
			name:     "with credentials and secure",
			nodeNum:  2,
			host:     "secure.clickhouse.com",
			port:     9440,
			username: "admin",
			password: "secret",
			secure:   true,
			checks: []string{
				"<host>secure.clickhouse.com</host>",
				"<port>9440</port>",
				"<user>admin</user>",
				"<password>secret</password>",
				"<secure>1</secure>",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			xml := generateSingleShardConfig(tt.nodeNum, tt.host, tt.port, tt.username, tt.password, tt.secure)

			for _, check := range tt.checks {
				assert.Contains(t, xml, check, "XML should contain: %s", check)
			}
		})
	}
}

func TestGenerateExternalClickHouseConfig_Integration(t *testing.T) {
	// Create a temporary directory for test output
	tmpDir, err := os.MkdirTemp("", "clickhouse-config-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Change to temp dir so relative paths work
	originalDir, err := os.Getwd()
	require.NoError(t, err)
	defer os.Chdir(originalDir)

	err = os.Chdir(tmpDir)
	require.NoError(t, err)

	// Create the source config directories that copyUsersConfig and copyInitDBScripts expect
	for nodeNum := 1; nodeNum <= 2; nodeNum++ {
		usersDir := filepath.Join("local-config", "clickhouse", formatNodeDir(nodeNum), "etc", "clickhouse-server", "users.d")
		err := os.MkdirAll(usersDir, 0o755)
		require.NoError(t, err)

		usersContent := `<clickhouse><users><default></default></users></clickhouse>`
		err = os.WriteFile(filepath.Join(usersDir, "users.xml"), []byte(usersContent), 0o644)
		require.NoError(t, err)

		initDBDir := filepath.Join("local-config", "clickhouse", formatNodeDir(nodeNum), "etc", "clickhouse-server", "docker-entrypoint-initdb.d")
		err = os.MkdirAll(initDBDir, 0o755)
		require.NoError(t, err)

		err = os.WriteFile(filepath.Join(initDBDir, "init.sh"), []byte("#!/bin/bash\necho init"), 0o755)
		require.NoError(t, err)
	}

	log := newTestLogger()

	t.Run("multi-shard config for known cluster", func(t *testing.T) {
		err := GenerateExternalClickHouseConfig(
			log,
			"chendpoint-xatu-clickhouse.analytics.production.ethpandaops",
			9000,
			"",
			"",
			false,
		)
		require.NoError(t, err)

		// Verify config files were created
		for nodeNum := 1; nodeNum <= 2; nodeNum++ {
			configPath := filepath.Join("local-config", "clickhouse-external", formatNodeDir(nodeNum), "etc", "clickhouse-server", "config.d", "config.xml")
			content, err := os.ReadFile(configPath)
			require.NoError(t, err, "config file should exist for node %d", nodeNum)

			xml := string(content)

			// Should have all 6 shard endpoints
			assert.Contains(t, xml, "chendpoint-xatu-clickhouse-0-0.analytics.production.ethpandaops")
			assert.Contains(t, xml, "chendpoint-xatu-clickhouse-0-1.analytics.production.ethpandaops")
			assert.Contains(t, xml, "chendpoint-xatu-clickhouse-1-0.analytics.production.ethpandaops")
			assert.Contains(t, xml, "chendpoint-xatu-clickhouse-1-1.analytics.production.ethpandaops")
			assert.Contains(t, xml, "chendpoint-xatu-clickhouse-2-0.analytics.production.ethpandaops")
			assert.Contains(t, xml, "chendpoint-xatu-clickhouse-2-1.analytics.production.ethpandaops")

			// Should have 3 shards in xatu_cluster
			xatuClusterStart := strings.Index(xml, "<xatu_cluster>")
			xatuClusterEnd := strings.Index(xml, "</xatu_cluster>")
			xatuClusterSection := xml[xatuClusterStart:xatuClusterEnd]
			assert.Equal(t, 3, strings.Count(xatuClusterSection, "<shard>"))
		}
	})

	t.Run("single-shard config for unknown cluster", func(t *testing.T) {
		err := GenerateExternalClickHouseConfig(
			log,
			"unknown-clickhouse.example.com",
			9000,
			"user",
			"pass",
			true,
		)
		require.NoError(t, err)

		configPath := filepath.Join("local-config", "clickhouse-external", "clickhouse-01", "etc", "clickhouse-server", "config.d", "config.xml")
		content, err := os.ReadFile(configPath)
		require.NoError(t, err)

		xml := string(content)

		// Should have single host
		assert.Contains(t, xml, "unknown-clickhouse.example.com")
		assert.Contains(t, xml, "<user>user</user>")
		assert.Contains(t, xml, "<password>pass</password>")
		assert.Contains(t, xml, "<secure>1</secure>")

		// Should have only 1 shard in xatu_cluster
		xatuClusterStart := strings.Index(xml, "<xatu_cluster>")
		xatuClusterEnd := strings.Index(xml, "</xatu_cluster>")
		xatuClusterSection := xml[xatuClusterStart:xatuClusterEnd]
		assert.Equal(t, 1, strings.Count(xatuClusterSection, "<shard>"))
	})
}

func TestGenerateExternalClickHouseConfig_EmptyHost(t *testing.T) {
	log := newTestLogger()

	err := GenerateExternalClickHouseConfig(log, "", 9000, "", "", false)
	assert.ErrorIs(t, err, errExternalHostRequired)
}

func TestGenerateExternalClickHouseConfigFromURL(t *testing.T) {
	// Create a temporary directory for test output
	tmpDir, err := os.MkdirTemp("", "clickhouse-config-url-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	originalDir, err := os.Getwd()
	require.NoError(t, err)
	defer os.Chdir(originalDir)

	err = os.Chdir(tmpDir)
	require.NoError(t, err)

	// Create source config directories
	for nodeNum := 1; nodeNum <= 2; nodeNum++ {
		usersDir := filepath.Join("local-config", "clickhouse", formatNodeDir(nodeNum), "etc", "clickhouse-server", "users.d")
		err := os.MkdirAll(usersDir, 0o755)
		require.NoError(t, err)
		err = os.WriteFile(filepath.Join(usersDir, "users.xml"), []byte("<clickhouse/>"), 0o644)
		require.NoError(t, err)
	}

	log := newTestLogger()

	tests := []struct {
		name        string
		url         string
		expectError error
		checkHost   string
		checkSecure string
	}{
		{
			name:        "http URL",
			url:         "http://clickhouse.example.com:9000",
			expectError: nil,
			checkHost:   "clickhouse.example.com",
			checkSecure: "<secure>0</secure>",
		},
		{
			name:        "https URL",
			url:         "https://secure.clickhouse.com:9440",
			expectError: nil,
			checkHost:   "secure.clickhouse.com",
			checkSecure: "<secure>1</secure>",
		},
		{
			name:        "URL with credentials",
			url:         "http://myuser:mypass@clickhouse.example.com:9000",
			expectError: nil,
			checkHost:   "clickhouse.example.com",
		},
		{
			name:        "empty URL",
			url:         "",
			expectError: errXatuURLEmpty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := GenerateExternalClickHouseConfigFromURL(log, tt.url)

			if tt.expectError != nil {
				assert.ErrorIs(t, err, tt.expectError)
				return
			}

			require.NoError(t, err)

			if tt.checkHost != "" {
				configPath := filepath.Join("local-config", "clickhouse-external", "clickhouse-01", "etc", "clickhouse-server", "config.d", "config.xml")
				content, err := os.ReadFile(configPath)
				require.NoError(t, err)

				xml := string(content)
				assert.Contains(t, xml, tt.checkHost)

				if tt.checkSecure != "" {
					assert.Contains(t, xml, tt.checkSecure)
				}
			}
		})
	}
}

func TestKnownClusters(t *testing.T) {
	// Verify the knownClusters map is properly configured
	cluster, ok := knownClusters["chendpoint-xatu-clickhouse"]
	require.True(t, ok, "chendpoint-xatu-clickhouse should be in knownClusters")

	assert.Equal(t, "chendpoint-xatu-clickhouse", cluster.HostPrefix)
	assert.Equal(t, 3, len(cluster.Shards), "should have 3 shards")

	// Verify shard structure
	expectedSuffixes := [][]string{
		{"-0-0", "-0-1"},
		{"-1-0", "-1-1"},
		{"-2-0", "-2-1"},
	}

	for i, shard := range cluster.Shards {
		require.Equal(t, 2, len(shard.Replicas), "shard %d should have 2 replicas", i)
		for j, replica := range shard.Replicas {
			assert.Equal(t, expectedSuffixes[i][j], replica.HostSuffix, "shard %d replica %d suffix mismatch", i, j)
		}
	}
}

// formatNodeDir returns the node directory name in the format "clickhouse-01", "clickhouse-02"
func formatNodeDir(nodeNum int) string {
	return "clickhouse-0" + string(rune('0'+nodeNum))
}
