package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type TestDefinition struct {
	Tests []Test `yaml:"tests"`
}

type Test struct {
	Name       string      `yaml:"name"`
	DataSet    string      `yaml:"data_set"`
	Assertions []Assertion `yaml:"assertions"`
}

type Assertion struct {
	Query    string                 `yaml:"query"`
	Expected map[string]interface{} `yaml:"expected"`
}

func main() {
	var (
		pattern        string
		databasePrefix string
	)

	flag.StringVar(&pattern, "pattern", "*.test.yaml", "Test file pattern")
	flag.StringVar(&databasePrefix, "database-prefix", "", "Database prefix for isolation")
	flag.Parse()

	log := logrus.New()
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	if databasePrefix == "" {
		log.Fatal("--database-prefix flag is required")
	}

	ctx := context.Background()

	if err := runAssertions(ctx, log, pattern, databasePrefix); err != nil {
		log.WithError(err).Fatal("Assertions failed")
	}
}

func runAssertions(ctx context.Context, log *logrus.Logger, pattern string, databasePrefix string) error {
	testFiles, err := filepath.Glob(filepath.Join("models", "**", pattern))
	if err != nil {
		return fmt.Errorf("failed to find test files: %w", err)
	}

	if len(testFiles) == 0 {
		log.WithField("pattern", pattern).Info("No test files found")
		return nil
	}

	dsn := fmt.Sprintf("clickhouse://localhost:9000/%smainnet?compress=true", databasePrefix)
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(time.Hour)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	totalPassed := 0
	totalFailed := 0

	for _, testFile := range testFiles {
		log.WithField("file", testFile).Info("Running test file")

		data, err := os.ReadFile(testFile)
		if err != nil {
			return fmt.Errorf("failed to read test file %s: %w", testFile, err)
		}

		var testDef TestDefinition
		if err := yaml.Unmarshal(data, &testDef); err != nil {
			return fmt.Errorf("failed to parse test file %s: %w", testFile, err)
		}

		for _, test := range testDef.Tests {
			log.WithField("test", test.Name).Info("Running test")

			for i, assertion := range test.Assertions {
				if err := runAssertion(ctx, db, assertion); err != nil {
					log.WithFields(logrus.Fields{
						"test":      test.Name,
						"assertion": i + 1,
						"query":     assertion.Query,
					}).WithError(err).Error("Assertion failed")
					totalFailed++
				} else {
					log.WithFields(logrus.Fields{
						"test":      test.Name,
						"assertion": i + 1,
					}).Debug("Assertion passed")
					totalPassed++
				}
			}
		}
	}

	log.WithFields(logrus.Fields{
		"passed": totalPassed,
		"failed": totalFailed,
	}).Info("Test run completed")

	if totalFailed > 0 {
		return fmt.Errorf("%d assertions failed", totalFailed)
	}

	return nil
}

func runAssertion(ctx context.Context, db *sql.DB, assertion Assertion) error {
	rows, err := db.QueryContext(ctx, assertion.Query)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	if !rows.Next() {
		return fmt.Errorf("no results returned")
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return fmt.Errorf("failed to scan row: %w", err)
	}

	for i, col := range columns {
		expected, ok := assertion.Expected[col]
		if !ok {
			continue
		}

		actual := values[i]

		actualValue := normalizeValue(actual)
		expectedValue := normalizeValue(expected)

		if !reflect.DeepEqual(actualValue, expectedValue) {
			return fmt.Errorf("column %s: expected %v (type %T), got %v (type %T)",
				col, expectedValue, expectedValue, actualValue, actualValue)
		}
	}

	return nil
}

func normalizeValue(v interface{}) interface{} {
	switch val := v.(type) {
	case []uint8:
		return string(val)
	case int64:
		return val
	case float64:
		return val
	case int:
		return int64(val)
	case float32:
		return float64(val)
	case uint64:
		return int64(val)
	case uint32:
		return int64(val)
	case nil:
		return nil
	default:
		return v
	}
}
