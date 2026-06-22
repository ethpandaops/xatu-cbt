package testing

import (
	"os"
	"path/filepath"
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

func TestLatestMigrationVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		files   []string
		want    int64
		wantErr bool
	}{
		{
			name:  "picks highest version across up files",
			files: []string{"001_admin.up.sql", "001_admin.down.sql", "079_x.up.sql", "012_y.up.sql"},
			want:  79,
		},
		{
			name:  "ignores down files and non-up sql",
			files: []string{"005_a.up.sql", "099_a.down.sql", "100_a.sql", "README.md"},
			want:  5,
		},
		{
			name:    "errors when no up migrations present",
			files:   []string{"003_a.down.sql", "notes.txt"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			for _, name := range tt.files {
				require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("SELECT 1;"), 0o600))
			}

			got, err := latestMigrationVersion(dir)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
