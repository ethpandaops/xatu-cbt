package cbt

// Config represents the structure of the CBT YAML configuration file.
// This is the format expected by the CBT Docker container.
type Config struct {
	ClickHouse struct {
		URL         string `yaml:"url"`
		Cluster     string `yaml:"cluster,omitempty"`
		LocalSuffix string `yaml:"localSuffix,omitempty"`
		Admin       struct {
			Incremental struct {
				Database string `yaml:"database"`
				Table    string `yaml:"table"`
			} `yaml:"incremental"`
			Scheduled struct {
				Database string `yaml:"database"`
				Table    string `yaml:"table"`
			} `yaml:"scheduled"`
		} `yaml:"admin"`
	} `yaml:"clickhouse"`
	Redis struct {
		URL    string `yaml:"url"`
		Prefix string `yaml:"prefix"`
	} `yaml:"redis"`
	Models struct {
		External struct {
			DefaultCluster  string   `yaml:"defaultCluster,omitempty"`
			DefaultDatabase string   `yaml:"defaultDatabase,omitempty"`
			Paths           []string `yaml:"paths"`
		} `yaml:"external"`
		Transformations struct {
			DefaultDatabase string   `yaml:"defaultDatabase,omitempty"`
			Paths           []string `yaml:"paths"`
		} `yaml:"transformations"`
		Env       map[string]string          `yaml:"env,omitempty"`
		Overrides map[string]*ModelOverrides `yaml:"overrides,omitempty"`
	} `yaml:"models"`
	Scheduler struct {
		Concurrency int `yaml:"concurrency"`
	} `yaml:"scheduler"`
	Worker struct {
		Concurrency int `yaml:"concurrency"`
	} `yaml:"worker"`
}

// ModelOverrides contains per-model configuration overrides.
type ModelOverrides struct {
	Config struct {
		Lag       *int              `yaml:"lag,omitempty"`
		Schedule  string            `yaml:"schedule,omitempty"`
		Schedules map[string]string `yaml:"schedules,omitempty"`
	} `yaml:"config"`
}
