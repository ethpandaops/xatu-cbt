package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

const (
	modelTypeExternal       = "external"
	modelTypeTransformation = "transformation"
	timestampOverflow       = "overflow"
)

var (
	// ErrInvalidPath indicates a path contains invalid characters or traversal attempts
	ErrInvalidPath = errors.New("invalid path")
	// ErrInvalidSQLFormat indicates a SQL file is missing required YAML frontmatter
	ErrInvalidSQLFormat = errors.New("invalid SQL file format: missing frontmatter")
	// ErrCircularDep indicates a circular dependency was detected in the model DAG
	ErrCircularDep = errors.New("circular dependency detected")
	// ErrModelNotFound indicates the specified model was not found
	ErrModelNotFound = errors.New("model not found")
)

// Model represents a CBT model with its metadata and dependencies
type Model struct {
	Table        string            `yaml:"table"`
	Dependencies []interface{}     `yaml:"dependencies"` // Can be strings or arrays of strings
	Cache        *ModelCache       `yaml:"cache,omitempty"`
	Interval     *ModelInterval    `yaml:"interval,omitempty"`
	Schedules    map[string]string `yaml:"schedules,omitempty"`
	Tags         []string          `yaml:"tags,omitempty"`
	Exec         string            `yaml:"exec,omitempty"`
}

// ModelCache represents cache configuration for external models
type ModelCache struct {
	IncrementalScanInterval string `yaml:"incremental_scan_interval"`
	FullScanInterval        string `yaml:"full_scan_interval"`
}

// ModelInterval represents interval configuration for transformation models
type ModelInterval struct {
	Max int64 `yaml:"max"`
}

// ModelBounds represents the bounds for a model from admin_cbt table
type ModelBounds struct {
	Database string
	Table    string
	MinPos   uint64
	MaxPos   uint64
}

// ExternalModelCache represents the cache data stored in Redis for external models
type ExternalModelCache struct {
	ModelID             string    `json:"model_id"`
	Min                 int64     `json:"min"`
	Max                 int64     `json:"max"`
	LastIncrementalScan time.Time `json:"last_incremental_scan"`
	LastFullScan        time.Time `json:"last_full_scan"`
	PreviousMin         int64     `json:"previous_min"`
	PreviousMax         int64     `json:"previous_max"`
	UpdatedAt           time.Time `json:"updated_at"`
}

// ModelNode represents a node in the dependency tree
type ModelNode struct {
	Name         string
	Type         string // "external" or "transformation"
	Model        *Model
	Bounds       *ModelBounds
	Dependencies []*ModelNode
	Level        int  // depth in the tree
	IsMinBlocker bool // true if this node constrains the minimum bound
	IsMaxBlocker bool // true if this node constrains the maximum bound
}

var boundsCmd = &cobra.Command{
	Use:   "bounds [model_name]",
	Short: "Display model bounds and dependencies",
	Long: `Display the current bounds of a transformation model and all its dependencies.

This command will:
1. Load the specified transformation model
2. Build a dependency DAG (Directed Acyclic Graph)
3. Query ClickHouse for current bounds from admin_cbt table
4. Display an ASCII tree diagram with bounds information`,
	Args: cobra.ExactArgs(1),
	RunE: runBounds,
}

func runBounds(_ *cobra.Command, args []string) error {
	modelName := args[0]
	Logger.WithField("model", modelName).Info("Analyzing model bounds and dependencies")

	// Get network from environment
	network := os.Getenv("NETWORK")
	if network == "" {
		return ErrNetworkNotSet
	}

	// Load all models
	externalModels, err := loadModels("models/external")
	if err != nil {
		return fmt.Errorf("failed to load external models: %w", err)
	}

	transformationModels, err := loadModels("models/transformations")
	if err != nil {
		return fmt.Errorf("failed to load transformation models: %w", err)
	}

	// Find the target model
	targetModel, found := transformationModels[modelName]
	if !found {
		return fmt.Errorf("%w: %s in transformations", ErrModelNotFound, modelName)
	}

	// Build dependency tree
	tree, err := buildDependencyTree(targetModel, modelName, modelTypeTransformation, externalModels, transformationModels, 0, make(map[string]bool))
	if err != nil {
		return fmt.Errorf("failed to build dependency tree: %w", err)
	}

	// Connect to ClickHouse and query bounds
	ctx := context.Background()
	conn, err := connectClickHouse(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			Logger.WithError(closeErr).Debug("Failed to close ClickHouse connection")
		}
	}()

	// Connect to Redis for external model bounds
	redisClient, err := connectRedis(ctx, network)
	if err != nil {
		Logger.WithError(err).Debug("Failed to connect to Redis, external model bounds unavailable")
		redisClient = nil
	} else {
		Logger.Debug("Successfully connected to Redis")
	}
	if redisClient != nil {
		defer func() {
			if err := redisClient.Close(); err != nil {
				Logger.WithError(err).Debug("Failed to close Redis connection")
			}
		}()
	}

	// Query bounds for all models in the tree
	if err := queryBounds(ctx, conn, redisClient, tree, network); err != nil {
		return fmt.Errorf("failed to query bounds: %w", err)
	}

	// Calculate effective ranges and identify blockers
	calculateBlockers(tree)

	// Display the tree
	fmt.Println()

	// Display effective range summary if the model has dependencies
	if len(tree.Dependencies) > 0 {
		effectiveRange := calculateEffectiveRange(tree)
		if effectiveRange != "" {
			fmt.Printf("📊 Effective processing range: %s\n\n", effectiveRange)
		}
	}

	displayTree(tree, "", true)
	fmt.Println()

	return nil
}

func loadModels(dir string) (map[string]*Model, error) {
	models := make(map[string]*Model)

	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	if err != nil {
		return nil, err
	}

	yamlFiles, err := filepath.Glob(filepath.Join(dir, "*.yml"))
	if err != nil {
		return nil, err
	}
	files = append(files, yamlFiles...)

	yamlFiles, err = filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	files = append(files, yamlFiles...)

	for _, file := range files {
		model, err := loadModel(file)
		if err != nil {
			Logger.WithError(err).WithField("file", file).Warn("Failed to load model")
			continue
		}
		if model.Table != "" {
			models[model.Table] = model
		}
	}

	return models, nil
}

func loadModel(path string) (*Model, error) {
	// Validate path to prevent directory traversal
	cleanPath := filepath.Clean(path)
	if strings.Contains(cleanPath, "..") {
		return nil, fmt.Errorf("%w: %s", ErrInvalidPath, path)
	}
	file, err := os.Open(cleanPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			Logger.WithError(closeErr).Debug("Failed to close file")
		}
	}()

	content, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	// Extract YAML frontmatter from SQL files
	if strings.HasSuffix(path, ".sql") {
		parts := strings.Split(string(content), "---")
		if len(parts) >= 3 {
			content = []byte(parts[1])
		} else {
			return nil, ErrInvalidSQLFormat
		}
	}

	var model Model
	if err := yaml.Unmarshal(content, &model); err != nil {
		return nil, err
	}

	return &model, nil
}

func buildDependencyTree(model *Model, name, modelType string, externalModels, transformationModels map[string]*Model, level int, visited map[string]bool) (*ModelNode, error) {
	// Check for cycles
	key := fmt.Sprintf("%s.%s", modelType, name)
	if visited[key] {
		return nil, fmt.Errorf("%w: %s", ErrCircularDep, key)
	}
	visited[key] = true
	defer delete(visited, key)

	node := &ModelNode{
		Name:  name,
		Type:  modelType,
		Model: model,
		Level: level,
	}

	// Process dependencies
	for _, dep := range model.Dependencies {
		// Handle both string dependencies and arrays of dependencies (OR dependencies)
		var depStrings []string

		switch v := dep.(type) {
		case string:
			// Simple string dependency
			depStrings = []string{v}
		case []interface{}:
			// Array of dependencies (OR dependencies) - process first available one
			for _, item := range v {
				if str, ok := item.(string); ok {
					depStrings = append(depStrings, str)
				}
			}
			// For OR dependencies, we'll show all options but only process the first found
		default:
			Logger.WithField("dependency", dep).Warn("Unsupported dependency type, skipping")
			continue
		}

		// Process each dependency string
		for _, depStr := range depStrings {
			// Parse dependency string (e.g., "{{external}}.table_name" or "{{transformation}}.table_name")
			depParts := strings.Split(depStr, ".")
			if len(depParts) != 2 {
				Logger.WithField("dependency", depStr).Warn("Invalid dependency format, skipping")
				continue
			}

			depType := strings.Trim(depParts[0], "{}")
			depName := depParts[1]

			var depModel *Model
			var depModelType string

			switch depType {
			case modelTypeExternal:
				depModel = externalModels[depName]
				depModelType = modelTypeExternal
			case modelTypeTransformation:
				depModel = transformationModels[depName]
				depModelType = modelTypeTransformation
			default:
				Logger.WithField("dependency", depStr).Warn("Unknown dependency type, skipping")
				continue
			}

			if depModel == nil {
				// For OR dependencies, try the next option
				if len(depStrings) > 1 {
					Logger.WithFields(logrus.Fields{
						"dependency": depStr,
						"type":       depType,
						"name":       depName,
					}).Debug("OR dependency not found, trying next option")
					continue
				}
				Logger.WithFields(logrus.Fields{
					"dependency": depStr,
					"type":       depType,
					"name":       depName,
				}).Warn("Dependency not found, skipping")
				continue
			}

			depNode, err := buildDependencyTree(depModel, depName, depModelType, externalModels, transformationModels, level+1, visited)
			if err != nil {
				return nil, err
			}

			node.Dependencies = append(node.Dependencies, depNode)

			// For OR dependencies, only use the first found dependency
			if len(depStrings) > 1 {
				break
			}
		}
	}

	// Sort dependencies for consistent display
	sort.Slice(node.Dependencies, func(i, j int) bool {
		return node.Dependencies[i].Name < node.Dependencies[j].Name
	})

	return node, nil
}

func connectRedis(ctx context.Context, _ string) (*redis.Client, error) {
	// Connect to Redis container based on network
	// Use 127.0.0.1 explicitly to avoid IPv6 issues
	redisHost := "127.0.0.1"
	redisPort := "6379"

	// Check if we should connect to a specific Redis container
	redisURL := fmt.Sprintf("redis://%s:%s/0", redisHost, redisPort)

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis URL: %w", err)
	}

	client := redis.NewClient(opt)

	// Test connection
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping Redis: %w", err)
	}

	return client, nil
}

func connectClickHouse(ctx context.Context) (clickhouse.Conn, error) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv("CLICKHOUSE_NATIVE_PORT")
	if port == "" {
		port = "9000"
	}

	username := os.Getenv("CLICKHOUSE_USERNAME")
	if username == "" {
		username = "default"
	}

	password := os.Getenv("CLICKHOUSE_PASSWORD")

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", host, port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: username,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	})

	if err != nil {
		return nil, err
	}

	// Test connection
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return conn, nil
}

func queryExternalModelBounds(ctx context.Context, redisClient *redis.Client, node *ModelNode, network string) {
	if redisClient == nil {
		return
	}

	redisKey := fmt.Sprintf("cbt:external:%s.%s", network, node.Name)
	val, err := redisClient.Get(ctx, redisKey).Result()

	if err != nil && !errors.Is(err, redis.Nil) {
		Logger.WithError(err).WithFields(logrus.Fields{
			"model":   node.Name,
			"network": network,
			"key":     redisKey,
		}).Debug("Failed to query Redis for external model bounds")
		return
	}

	if err != nil {
		return
	}

	// Parse the Redis JSON data
	var cache ExternalModelCache
	if err := json.Unmarshal([]byte(val), &cache); err != nil {
		Logger.WithError(err).WithField("model", node.Name).Warn("Failed to parse Redis cache data")
		return
	}

	// Convert to ModelBounds with overflow checking
	minPos := uint64(0)
	if cache.Min >= 0 {
		minPos = uint64(cache.Min)
	}
	maxPos := uint64(0)
	if cache.Max >= 0 {
		maxPos = uint64(cache.Max)
	}

	node.Bounds = &ModelBounds{
		Database: network,
		Table:    node.Name,
		MinPos:   minPos,
		MaxPos:   maxPos,
	}
}

func queryTransformationModelBounds(ctx context.Context, conn clickhouse.Conn, node *ModelNode, network string) {
	// Use backticks to properly escape database names with special characters
	query := fmt.Sprintf(`
		SELECT 
			database,
			table,
			min(position) as min_pos,
			max(position + interval) as max_pos
		FROM `+"`%s`"+`.admin_cbt
		WHERE database = '%s' AND table = '%s'
		GROUP BY database, table
	`, network, network, node.Name)

	var bounds ModelBounds
	err := conn.QueryRow(ctx, query).Scan(
		&bounds.Database,
		&bounds.Table,
		&bounds.MinPos,
		&bounds.MaxPos,
	)

	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		Logger.WithError(err).WithFields(logrus.Fields{
			"model":   node.Name,
			"network": network,
		}).Warn("Failed to query bounds")
	}

	if err == nil {
		node.Bounds = &bounds
	}
}

func calculateEffectiveRange(node *ModelNode) string {
	if len(node.Dependencies) == 0 {
		return ""
	}

	// Find the effective range constraints from dependencies
	var maxOfMins uint64
	var minOfMaxes = ^uint64(0) // Max uint64 value
	hasAnyBounds := false

	for _, dep := range node.Dependencies {
		if dep.Bounds != nil {
			hasAnyBounds = true
			if dep.Bounds.MinPos > maxOfMins {
				maxOfMins = dep.Bounds.MinPos
			}
			if dep.Bounds.MaxPos < minOfMaxes {
				minOfMaxes = dep.Bounds.MaxPos
			}
		}
	}

	if !hasAnyBounds || minOfMaxes == ^uint64(0) {
		return ""
	}

	// Check if there's a valid range
	if maxOfMins > minOfMaxes {
		return fmt.Sprintf("❌ No valid range! Min constraint (%s) > Max constraint (%s)",
			formatTimestamp(maxOfMins), formatTimestamp(minOfMaxes))
	}

	return fmt.Sprintf("%s → %s", formatTimestamp(maxOfMins), formatTimestamp(minOfMaxes))
}

func formatTimestamp(ts uint64) string {
	const maxInt64 = 1<<63 - 1
	if ts <= maxInt64 {
		return time.Unix(int64(ts), 0).UTC().Format("2006-01-02 15:04:05")
	}
	return timestampOverflow
}

func calculateBlockers(node *ModelNode) {
	if len(node.Dependencies) == 0 {
		return
	}

	// First, recursively calculate blockers for all dependencies
	for _, dep := range node.Dependencies {
		calculateBlockers(dep)
	}

	// Find the effective range constraints from dependencies
	var maxOfMins uint64
	var minOfMaxes = ^uint64(0) // Max uint64 value
	hasAnyBounds := false

	// Calculate max of mins and min of maxes
	for _, dep := range node.Dependencies {
		if dep.Bounds != nil {
			hasAnyBounds = true
			if dep.Bounds.MinPos > maxOfMins {
				maxOfMins = dep.Bounds.MinPos
			}
			if dep.Bounds.MaxPos < minOfMaxes {
				minOfMaxes = dep.Bounds.MaxPos
			}
		}
	}

	if !hasAnyBounds {
		return
	}

	// Mark dependencies that are blockers
	for _, dep := range node.Dependencies {
		if dep.Bounds != nil {
			// Check if this dependency constrains the minimum (is the latest start)
			if dep.Bounds.MinPos == maxOfMins {
				dep.IsMinBlocker = true
			}
			// Check if this dependency constrains the maximum (is the earliest end)
			if dep.Bounds.MaxPos == minOfMaxes {
				dep.IsMaxBlocker = true
			}
		}
	}
}

func queryBounds(ctx context.Context, conn clickhouse.Conn, redisClient *redis.Client, node *ModelNode, network string) error {
	// Query bounds based on model type
	switch node.Type {
	case modelTypeExternal:
		queryExternalModelBounds(ctx, redisClient, node, network)
	case modelTypeTransformation:
		queryTransformationModelBounds(ctx, conn, node, network)
	}

	// Recursively query bounds for dependencies
	for _, dep := range node.Dependencies {
		if err := queryBounds(ctx, conn, redisClient, dep, network); err != nil {
			return err
		}
	}

	return nil
}

func formatBounds(bounds *ModelBounds) string {
	if bounds == nil {
		return "no bounds"
	}

	// Handle potential overflow by checking bounds
	const maxInt64 = 1<<63 - 1
	var minTime, maxTime string

	if bounds.MinPos <= maxInt64 {
		minTime = time.Unix(int64(bounds.MinPos), 0).UTC().Format("2006-01-02 15:04:05")
	} else {
		minTime = timestampOverflow
	}

	if bounds.MaxPos <= maxInt64 {
		maxTime = time.Unix(int64(bounds.MaxPos), 0).UTC().Format("2006-01-02 15:04:05")
	} else {
		maxTime = timestampOverflow
	}

	return fmt.Sprintf("[%s → %s]", minTime, maxTime)
}

func displayTree(node *ModelNode, prefix string, isLast bool) {
	// ANSI color codes
	const (
		colorRed   = "\033[31m"
		colorReset = "\033[0m"
	)

	// Prepare the connector
	connector := "├── "
	if isLast {
		connector = "└── "
	}
	if prefix == "" {
		connector = ""
	}

	// Format bounds information
	boundsInfo := formatBounds(node.Bounds)

	// Apply red color if this node is a blocker
	if node.IsMinBlocker || node.IsMaxBlocker {
		boundsInfo = colorRed + boundsInfo + colorReset
	}

	// Display node with color coding based on type
	nodeTypeIndicator := ""
	if node.Type == modelTypeExternal {
		nodeTypeIndicator = "(ext) "
	}

	fmt.Printf("%s%s%s%s %s\n", prefix, connector, nodeTypeIndicator, node.Name, boundsInfo)

	// Prepare prefix for children
	var childPrefix string
	if prefix != "" {
		if isLast {
			childPrefix = prefix + "    "
		} else {
			childPrefix = prefix + "│   "
		}
	}

	// Display dependencies
	for i, dep := range node.Dependencies {
		displayTree(dep, childPrefix, i == len(node.Dependencies)-1)
	}
}

func init() {
	rootCmd.AddCommand(boundsCmd)
}
