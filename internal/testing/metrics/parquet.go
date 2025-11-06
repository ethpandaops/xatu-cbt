package metrics

import "time"

// ParquetLoadSource indicates where parquet data was loaded from
type ParquetLoadSource string

const (
	SourceCache ParquetLoadSource = "cache"
	SourceS3    ParquetLoadSource = "s3"
)

// ParquetLoadMetric captures metrics about loading a parquet file
type ParquetLoadMetric struct {
	Table     string
	Source    ParquetLoadSource // cache or s3
	SizeBytes int64
	Duration  time.Duration
	Timestamp time.Time
}
