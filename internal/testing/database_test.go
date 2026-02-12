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
