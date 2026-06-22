package testing

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModifyCreateTableForClone_CrossDatabaseRenameDoesNotDoublePrefix(t *testing.T) {
	t.Parallel()

	manager := &DatabaseManager{}
	createStmt := "CREATE TABLE `observoor`.`cpu_utilization` (\n" +
		"    `value` UInt64\n" +
		") ENGINE = Distributed('{cluster}', 'observoor', 'cpu_utilization_local', rand())"

	modified := manager.modifyCreateTableForClone(
		createStmt,
		"observoor",
		"ext_123",
		"cpu_utilization",
		"observoor_cpu_utilization",
		"xatu",
	)

	require.Contains(t, modified, "CREATE TABLE IF NOT EXISTS `ext_123`.`observoor_cpu_utilization` ON CLUSTER xatu")
	require.Contains(t, modified, "'observoor_cpu_utilization_local'")
	require.NotContains(t, modified, "observoor_observoor_cpu_utilization")
}

func TestScopedDSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		connStr string
		dbName  string
		want    string
	}{
		{
			name:    "adds database path to bare dsn",
			connStr: "clickhouse://default:supersecret@localhost:9000",
			dbName:  "cbt_template",
			want:    "clickhouse://default:supersecret@localhost:9000/cbt_template",
		},
		{
			name:    "overrides an existing database path",
			connStr: "clickhouse://default:supersecret@localhost:9000/default",
			dbName:  "mainnet",
			want:    "clickhouse://default:supersecret@localhost:9000/mainnet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := scopedDSN(tt.connStr, tt.dbName)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
