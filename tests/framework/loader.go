package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type DataSet struct {
	Name        string      `yaml:"name"`
	Description string      `yaml:"description"`
	Network     string      `yaml:"network"`
	Tables      []Table     `yaml:"tables"`
	Assertions  []Assertion `yaml:"assertions"`
}

type Table struct {
	Name string   `yaml:"name"`
	URLs []string `yaml:"urls"`
}

type Assertion struct {
	Slot             int    `yaml:"slot"`
	ExpectedProposer int    `yaml:"expected_proposer"`
	ExpectedRoot     string `yaml:"expected_block_root"`
}

func main() {
	var (
		dataSetPath    string
		databasePrefix string
	)

	flag.StringVar(&dataSetPath, "data-set", "", "Path to data set YAML file")
	flag.StringVar(&databasePrefix, "database-prefix", "", "Database prefix for isolation")
	flag.Parse()

	log := logrus.New()
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	if dataSetPath == "" {
		log.Fatal("--data-set flag is required")
	}

	if databasePrefix == "" {
		log.Fatal("--database-prefix flag is required")
	}

	ctx := context.Background()

	if err := loadDataSet(ctx, log, dataSetPath, databasePrefix); err != nil {
		log.WithError(err).Fatal("Failed to load data set")
	}
}

func loadDataSet(ctx context.Context, log *logrus.Logger, dataSetPath string, databasePrefix string) error {
	data, err := os.ReadFile(dataSetPath)
	if err != nil {
		return fmt.Errorf("failed to read data set file: %w", err)
	}

	var dataSet DataSet
	if err := yaml.Unmarshal(data, &dataSet); err != nil {
		return fmt.Errorf("failed to parse data set YAML: %w", err)
	}

	log.WithField("dataset", dataSet.Name).Info("Loading data set")

	dsn := fmt.Sprintf("clickhouse://localhost:9000/%smainnet?compress=true", databasePrefix)
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	for _, table := range dataSet.Tables {
		for _, url := range table.URLs {
			log.WithFields(logrus.Fields{
				"table": table.Name,
				"url":   url,
			}).Info("Loading parquet file")

			// Tables already exist from xatu migrations
			// Just insert into the existing table structure
			tableName := fmt.Sprintf("%s%s.%s", databasePrefix, dataSet.Network, table.Name)

			query := fmt.Sprintf(`
				INSERT INTO %s
				SELECT * FROM url('%s', 'Parquet')
			`, tableName, url)

			if _, err := db.ExecContext(ctx, query); err != nil {
				return fmt.Errorf("failed to load data from %s into %s: %w", url, tableName, err)
			}

			var count int
			countQuery := fmt.Sprintf("SELECT count(*) FROM %s", tableName)
			if err := db.QueryRowContext(ctx, countQuery).Scan(&count); err != nil {
				return fmt.Errorf("failed to count rows in %s: %w", tableName, err)
			}

			log.WithFields(logrus.Fields{
				"table": tableName,
				"rows":  count,
			}).Info("Data loaded successfully")
		}
	}

	log.Info("Data set loading completed")
	return nil
}
