package testing

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestParseModel_CrossDatabaseKeepsFilenameAsModelName(t *testing.T) {
	t.Parallel()

	modelPath := filepath.Join(t.TempDir(), "observoor_cpu_utilization.sql")

	content := `---
database: observoor
table: cpu_utilization
---
SELECT 1
`
	require.NoError(t, os.WriteFile(modelPath, []byte(content), 0o600))

	cache := NewModelCache(logrus.New())
	model, err := cache.parseModel(modelPath, ModelTypeExternal)
	require.NoError(t, err)

	require.Equal(t, "observoor_cpu_utilization", model.Name)
	require.Equal(t, "observoor", model.SourceDB)
	require.Equal(t, "cpu_utilization", model.SourceTable)
}
