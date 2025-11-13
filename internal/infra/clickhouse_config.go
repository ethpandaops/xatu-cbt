// Package infra provides infrastructure management for Docker and ClickHouse clusters.
package infra

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"

	"github.com/sirupsen/logrus"
)

var (
	errExternalHostRequired = errors.New("external host is required for external mode")
	errXatuURLEmpty         = errors.New("xatu URL is empty")
	errNoHostInXatuURL      = errors.New("no host found in xatu URL")
)

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
func GenerateExternalClickHouseConfig(log logrus.FieldLogger, host string, port int, username, password string, secure bool) error {
	if host == "" {
		return errExternalHostRequired
	}

	log.WithFields(logrus.Fields{
		"host":   host,
		"port":   port,
		"secure": secure,
	}).Info("generating ClickHouse external configuration")

	// Convert secure bool to int (0 or 1)
	secureInt := 0
	if secure {
		secureInt = 1
	}

	// Prepare username and password XML fragments
	usernameXML := ""
	if username != "" {
		usernameXML = fmt.Sprintf("\n                    <user>%s</user>", username)
	}

	passwordXML := ""
	if password != "" {
		passwordXML = fmt.Sprintf("\n                    <password>%s</password>", password)
	}

	// Base directory for clickhouse-external configs
	baseDir := filepath.Join("local-config", "clickhouse-external")

	// Generate configs for both ClickHouse nodes (01 and 02)
	for nodeNum := 1; nodeNum <= 2; nodeNum++ {
		// Generate config content with external cluster settings
		configContent := fmt.Sprintf(clickhouseConfigTemplate,
			nodeNum,           // display_name node number
			host,              // xatu_cluster host
			port,              // xatu_cluster port
			usernameXML,       // optional username
			passwordXML,       // optional password
			secureInt,         // secure flag
			nodeNum,           // replica number
		)

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
			"node": nodeNum,
			"path": configPath,
		}).Debug("wrote ClickHouse external config")
	}

	// Copy users.xml from local clickhouse config (same for both modes)
	if err := copyUsersConfig(log, baseDir); err != nil {
		return fmt.Errorf("failed to copy users config: %w", err)
	}

	log.Info("ClickHouse external configuration generated successfully")

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
