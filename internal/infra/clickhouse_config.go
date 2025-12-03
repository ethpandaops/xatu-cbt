// Package infra provides infrastructure management for Docker and ClickHouse clusters.
package infra

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"

	"github.com/sirupsen/logrus"
)

var (
	errExternalHostRequired = errors.New("external host is required for external mode")
	errXatuURLEmpty         = errors.New("xatu URL is empty")
	errNoHostInXatuURL      = errors.New("no host found in xatu URL")
)

// ReplicaDefinition defines a single replica endpoint suffix.
type ReplicaDefinition struct {
	HostSuffix string // e.g., "-0-0", "-0-1"
}

// ShardDefinition defines a shard with its replicas.
type ShardDefinition struct {
	Replicas []ReplicaDefinition
}

// ClusterTopology defines the complete topology of a known cluster.
type ClusterTopology struct {
	HostPrefix string            // e.g., "chendpoint-xatu-clickhouse"
	Shards     []ShardDefinition // Shard definitions with replica suffixes
}

// knownClusters maps hostname prefixes to their cluster topologies.
// When a hostname matches a prefix, the full topology is used for config generation.
var knownClusters = map[string]ClusterTopology{
	"chendpoint-xatu-clickhouse": {
		HostPrefix: "chendpoint-xatu-clickhouse",
		Shards: []ShardDefinition{
			{Replicas: []ReplicaDefinition{{HostSuffix: "-0-0"}, {HostSuffix: "-0-1"}}},
			{Replicas: []ReplicaDefinition{{HostSuffix: "-1-0"}, {HostSuffix: "-1-1"}}},
			{Replicas: []ReplicaDefinition{{HostSuffix: "-2-0"}, {HostSuffix: "-2-1"}}},
		},
	},
}

// ExpandedTopology contains the resolved cluster topology with full hostnames.
type ExpandedTopology struct {
	Shards []ExpandedShard
}

// ExpandedShard contains fully resolved replica hostnames for a shard.
type ExpandedShard struct {
	Replicas []string // Full hostnames, e.g., "chendpoint-xatu-clickhouse-0-0.analytics.production.ethpandaops"
}

// detectClusterTopology checks if the hostname matches a known cluster pattern.
// If matched, returns the expanded topology with full hostnames.
// Returns nil if no known cluster pattern matches.
func detectClusterTopology(log logrus.FieldLogger, hostname string) *ExpandedTopology {
	// Find the first dot to separate prefix from domain suffix
	dotIdx := strings.Index(hostname, ".")
	if dotIdx == -1 {
		log.WithField("hostname", hostname).Debug("no domain suffix found, using single-node config")
		return nil
	}

	prefix := hostname[:dotIdx]
	domainSuffix := hostname[dotIdx:] // includes the leading dot

	topology, ok := knownClusters[prefix]
	if !ok {
		log.WithFields(logrus.Fields{
			"hostname": hostname,
			"prefix":   prefix,
		}).Debug("hostname does not match any known cluster pattern")
		return nil
	}

	log.WithFields(logrus.Fields{
		"hostname":     hostname,
		"prefix":       prefix,
		"domainSuffix": domainSuffix,
		"shardCount":   len(topology.Shards),
	}).Info("detected known cluster topology")

	// Expand the topology with full hostnames
	expanded := &ExpandedTopology{
		Shards: make([]ExpandedShard, len(topology.Shards)),
	}

	for i, shard := range topology.Shards {
		expanded.Shards[i].Replicas = make([]string, len(shard.Replicas))
		for j, replica := range shard.Replicas {
			expanded.Shards[i].Replicas[j] = topology.HostPrefix + replica.HostSuffix + domainSuffix
		}
	}

	return expanded
}

// multiShardConfigTemplate is the XML template for multi-shard external clusters.
const multiShardConfigTemplate = `<clickhouse replace="true">
    <logger>
        <level>debug</level>
        <console>1</console>
    </logger>
    <display_name>cluster_2S_1R node {{.NodeNum}}</display_name>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <user_directories>
        <users_xml>
            <path>users.xml</path>
        </users_xml>
        <local_directory>
            <path>/var/lib/clickhouse/access/</path>
        </local_directory>
    </user_directories>
    <distributed_ddl>
        <path>/clickhouse/task_queue/ddl</path>
    </distributed_ddl>
    <remote_servers>
        <cluster_2S_1R>
            <secret>supersecret</secret>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>xatu-cbt-clickhouse-01</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>xatu-cbt-clickhouse-02</host>
                    <port>9000</port>
                </replica>
            </shard>
        </cluster_2S_1R>
        <xatu_cluster>
{{- range .Shards}}
            <shard>
                <internal_replication>true</internal_replication>
{{- range .Replicas}}
                <replica>
                    <host>{{.Host}}</host>
                    <port>{{$.Port}}</port>
{{- if $.Username}}
                    <user>{{$.Username}}</user>
{{- end}}
{{- if $.Password}}
                    <password>{{$.Password}}</password>
{{- end}}
                    <secure>{{$.Secure}}</secure>
                </replica>
{{- end}}
            </shard>
{{- end}}
        </xatu_cluster>
    </remote_servers>
    <zookeeper>
        <node>
            <host>xatu-cbt-clickhouse-zookeeper-01</host>
            <port>2181</port>
        </node>
        <node>
            <host>xatu-cbt-clickhouse-zookeeper-02</host>
            <port>2181</port>
        </node>
        <node>
            <host>xatu-cbt-clickhouse-zookeeper-03</host>
            <port>2181</port>
        </node>
    </zookeeper>
    <macros>
        <installation>xatu</installation>
        <cluster>cluster_2S_1R</cluster>
        <shard>01</shard>
        <replica>{{printf "%02d" .NodeNum}}</replica>
        <remote_cluster>xatu_cluster</remote_cluster>
    </macros>
    <access_control_improvements>
        <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
    </access_control_improvements>
</clickhouse>
`

// multiShardTemplateData holds the data for rendering the multi-shard template.
type multiShardTemplateData struct {
	NodeNum  int
	Port     int
	Username string
	Password string
	Secure   int // 0 or 1
	Shards   []templateShard
}

type templateShard struct {
	Replicas []templateReplica
}

type templateReplica struct {
	Host string
}

// generateMultiShardXML generates the ClickHouse XML config for a multi-shard cluster.
func generateMultiShardXML(nodeNum int, topology *ExpandedTopology, port int, username, password string, secure bool) (string, error) {
	secureInt := 0
	if secure {
		secureInt = 1
	}

	// Convert topology to template data
	shards := make([]templateShard, len(topology.Shards))
	for i, shard := range topology.Shards {
		replicas := make([]templateReplica, len(shard.Replicas))
		for j, host := range shard.Replicas {
			replicas[j] = templateReplica{Host: host}
		}
		shards[i] = templateShard{Replicas: replicas}
	}

	data := multiShardTemplateData{
		NodeNum:  nodeNum,
		Port:     port,
		Username: username,
		Password: password,
		Secure:   secureInt,
		Shards:   shards,
	}

	tmpl, err := template.New("config").Parse(multiShardConfigTemplate)
	if err != nil {
		return "", fmt.Errorf("parsing multi-shard template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("executing multi-shard template: %w", err)
	}

	return buf.String(), nil
}

// generateSingleShardConfig generates the existing single-shard config (for backward compatibility).
func generateSingleShardConfig(nodeNum int, host string, port int, username, password string, secure bool) string {
	secureInt := 0
	if secure {
		secureInt = 1
	}

	usernameXML := ""
	if username != "" {
		usernameXML = fmt.Sprintf("\n                    <user>%s</user>", username)
	}

	passwordXML := ""
	if password != "" {
		passwordXML = fmt.Sprintf("\n                    <password>%s</password>", password)
	}

	return fmt.Sprintf(clickhouseConfigTemplate,
		nodeNum,     // display_name node number
		host,        // xatu_cluster host
		port,        // xatu_cluster port
		usernameXML, // optional username
		passwordXML, // optional password
		secureInt,   // secure flag
		nodeNum,     // replica number
	)
}

// clickhouseConfigTemplate is the XML configuration template for ClickHouse external mode.
const clickhouseConfigTemplate = `<clickhouse replace="true">
    <logger>
        <level>debug</level>
        <console>1</console>
    </logger>
    <display_name>cluster_2S_1R node %d</display_name>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <user_directories>
        <users_xml>
            <path>users.xml</path>
        </users_xml>
        <local_directory>
            <path>/var/lib/clickhouse/access/</path>
        </local_directory>
    </user_directories>
    <distributed_ddl>
        <path>/clickhouse/task_queue/ddl</path>
    </distributed_ddl>
    <remote_servers>
        <cluster_2S_1R>
            <secret>supersecret</secret>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>xatu-cbt-clickhouse-01</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>xatu-cbt-clickhouse-02</host>
                    <port>9000</port>
                </replica>
            </shard>
        </cluster_2S_1R>
        <xatu_cluster>
            <shard>
                <internal_replication>false</internal_replication>
                <replica>
                    <host>%s</host>
                    <port>%d</port>%s%s
                    <secure>%d</secure>
                </replica>
            </shard>
        </xatu_cluster>
    </remote_servers>
    <zookeeper>
        <node>
            <host>xatu-cbt-clickhouse-zookeeper-01</host>
            <port>2181</port>
        </node>
        <node>
            <host>xatu-cbt-clickhouse-zookeeper-02</host>
            <port>2181</port>
        </node>
        <node>
            <host>xatu-cbt-clickhouse-zookeeper-03</host>
            <port>2181</port>
        </node>
    </zookeeper>
    <macros>
        <installation>xatu</installation>
        <cluster>cluster_2S_1R</cluster>
        <shard>01</shard>
        <replica>%02d</replica>
        <remote_cluster>xatu_cluster</remote_cluster>
    </macros>
    <access_control_improvements>
        <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
    </access_control_improvements>
</clickhouse>
`

// GenerateExternalClickHouseConfig generates ClickHouse XML configuration files for external mode.
// This creates config files in local-config/clickhouse-external/ with the xatu_cluster configured
// to point to the provided external ClickHouse connection details.
//
// For known cluster patterns (e.g., chendpoint-xatu-clickhouse), it automatically expands
// to include all shards and replicas. For unknown clusters, it uses a single-endpoint config.
func GenerateExternalClickHouseConfig(log logrus.FieldLogger, host string, port int, username, password string, secure bool) error {
	if host == "" {
		return errExternalHostRequired
	}

	log.WithFields(logrus.Fields{
		"host":   host,
		"port":   port,
		"secure": secure,
	}).Info("generating ClickHouse external configuration")

	// Check if this is a known cluster that should be expanded to multiple shards
	topology := detectClusterTopology(log, host)

	// Base directory for clickhouse-external configs
	baseDir := filepath.Join("local-config", "clickhouse-external")

	// Generate configs for both ClickHouse nodes (01 and 02)
	for nodeNum := 1; nodeNum <= 2; nodeNum++ {
		var configContent string
		var err error

		if topology != nil {
			// Known cluster - generate multi-shard config
			log.WithField("node", nodeNum).Debug("generating multi-shard configuration")
			configContent, err = generateMultiShardXML(nodeNum, topology, port, username, password, secure)
			if err != nil {
				return fmt.Errorf("generating multi-shard config for node %d: %w", nodeNum, err)
			}
		} else {
			// Unknown cluster - use existing single-shard logic
			log.WithField("node", nodeNum).Debug("generating single-shard configuration")
			configContent = generateSingleShardConfig(nodeNum, host, port, username, password, secure)
		}

		// Create directory structure
		nodeDir := filepath.Join(baseDir, fmt.Sprintf("clickhouse-%02d", nodeNum), "etc", "clickhouse-server", "config.d")
		//nolint:gosec // G301: Directory permissions 0o755 are intentional for ClickHouse config
		if err := os.MkdirAll(nodeDir, 0o755); err != nil {
			return fmt.Errorf("failed to create config directory for node %d: %w", nodeNum, err)
		}

		// Write config.xml
		configPath := filepath.Join(nodeDir, "config.xml")
		//nolint:gosec // G306: File permissions 0o644 are intentional for ClickHouse config
		if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
			return fmt.Errorf("failed to write config for node %d: %w", nodeNum, err)
		}

		log.WithFields(logrus.Fields{
			"node":       nodeNum,
			"path":       configPath,
			"multiShard": topology != nil,
		}).Debug("wrote ClickHouse external config")
	}

	// Copy users.xml from local clickhouse config (same for both modes)
	if err := copyUsersConfig(log, baseDir); err != nil {
		return fmt.Errorf("failed to copy users config: %w", err)
	}

	// Generate init-db scripts for external mode (without cluster config override)
	if err := generateInitDBScripts(log, baseDir); err != nil {
		return fmt.Errorf("failed to generate init-db scripts: %w", err)
	}

	if topology != nil {
		log.WithField("shardCount", len(topology.Shards)).Info("ClickHouse external configuration generated with multi-shard topology")
	} else {
		log.Info("ClickHouse external configuration generated with single-shard topology")
	}

	return nil
}

// GenerateExternalClickHouseConfigFromURL generates ClickHouse configuration by parsing a connection URL.
// Supports formats:
// - http://host:port
// - http://username:password@host:port
// - https://host:port (sets secure=true)
func GenerateExternalClickHouseConfigFromURL(log logrus.FieldLogger, xatuURL string) error {
	if xatuURL == "" {
		return errXatuURLEmpty
	}

	// Parse URL
	parsedURL, err := url.Parse(xatuURL)
	if err != nil {
		return fmt.Errorf("failed to parse xatu URL: %w", err)
	}

	host := parsedURL.Hostname()
	if host == "" {
		return errNoHostInXatuURL
	}

	// Determine secure flag based on scheme
	secure := parsedURL.Scheme == "https"

	// Parse port (default to 9000 if not specified)
	portStr := parsedURL.Port()
	port := 9000
	if portStr != "" {
		parsedPort, parseErr := strconv.Atoi(portStr)
		if parseErr != nil {
			return fmt.Errorf("invalid port in xatu URL: %w", parseErr)
		}
		port = parsedPort
	}

	// Extract username and password if present
	username := ""
	password := ""
	if parsedURL.User != nil {
		username = parsedURL.User.Username()
		password, _ = parsedURL.User.Password()
	}

	// Call the existing generation function
	return GenerateExternalClickHouseConfig(log, host, port, username, password, secure)
}

// externalInitDBScript is the init script for external mode.
// Unlike the local mode init script, this does NOT create a users.xml in config.d
// because the cluster topology is already defined in our generated config.xml.
const externalInitDBScript = `#!/bin/bash
set -e

cat /etc/clickhouse-server/users.d/users.xml

cat <<EOT >> /etc/clickhouse-server/users.d/default.xml
<yandex>
  <users>
    <${CLICKHOUSE_USER}>
      <profile>default</profile>
      <networks>
        <ip>::/0</ip>
      </networks>
      $([ -n "${CLICKHOUSE_PASSWORD}" ] && echo "<password>${CLICKHOUSE_PASSWORD}</password>")
      <quota>default</quota>
    </${CLICKHOUSE_USER}>
    <readonly>
      <password>${CLICKHOUSE_USER_READONLY_PASSWORD}</password>
    </readonly>
  </users>
</yandex>
EOT
`

// generateInitDBScripts creates init scripts for external mode.
// These scripts only handle user configuration, not cluster topology.
func generateInitDBScripts(log logrus.FieldLogger, destBaseDir string) error {
	for nodeNum := 1; nodeNum <= 2; nodeNum++ {
		destDir := filepath.Join(destBaseDir, fmt.Sprintf("clickhouse-%02d", nodeNum), "etc", "clickhouse-server", "docker-entrypoint-initdb.d")

		// Create destination directory
		//nolint:gosec // G301: Directory permissions 0o755 are intentional for ClickHouse config
		if err := os.MkdirAll(destDir, 0o755); err != nil {
			return fmt.Errorf("failed to create init-db directory for node %d: %w", nodeNum, err)
		}

		// Write the external init script
		destPath := filepath.Join(destDir, "init-db.sh")
		//nolint:gosec // G306: File permissions 0o755 for shell scripts
		if err := os.WriteFile(destPath, []byte(externalInitDBScript), 0o755); err != nil {
			return fmt.Errorf("failed to write init-db.sh for node %d: %w", nodeNum, err)
		}

		log.WithField("node", nodeNum).Debug("generated external init-db script")
	}

	return nil
}

// copyUsersConfig copies the users.xml configuration from clickhouse (local) to clickhouse-external.
// The users.xml is the same for both local and external modes - it's the user authentication config.
func copyUsersConfig(log logrus.FieldLogger, destBaseDir string) error {
	sourceBase := filepath.Join("local-config", "clickhouse")

	for nodeNum := 1; nodeNum <= 2; nodeNum++ {
		// Source and destination paths
		sourcePath := filepath.Join(sourceBase, fmt.Sprintf("clickhouse-%02d", nodeNum), "etc", "clickhouse-server", "users.d", "users.xml")
		destDir := filepath.Join(destBaseDir, fmt.Sprintf("clickhouse-%02d", nodeNum), "etc", "clickhouse-server", "users.d")
		destPath := filepath.Join(destDir, "users.xml")

		// Read source file
		//nolint:gosec // G304: sourcePath is constructed from constants and controlled variables
		usersContent, err := os.ReadFile(sourcePath)
		if err != nil {
			return fmt.Errorf("failed to read users config for node %d: %w", nodeNum, err)
		}

		// Create destination directory
		//nolint:gosec // G301: Directory permissions 0o755 are intentional for ClickHouse config
		if err := os.MkdirAll(destDir, 0o755); err != nil {
			return fmt.Errorf("failed to create users config directory for node %d: %w", nodeNum, err)
		}

		// Write to destination
		//nolint:gosec // G306: File permissions 0o644 are intentional for ClickHouse config
		if err := os.WriteFile(destPath, usersContent, 0o644); err != nil {
			return fmt.Errorf("failed to write users config for node %d: %w", nodeNum, err)
		}

		log.WithField("node", nodeNum).Debug("copied users.xml config")
	}

	return nil
}
