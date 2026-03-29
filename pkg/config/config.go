package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-yaml"
)

// Config は swarun の全設定を保持する構造体です。
type Config struct {
	Port           int           `yaml:"port"`
	ControllerAddr string        `yaml:"controller_addr"`
	WorkerID       string        `yaml:"worker_id"`
	LogLevel       string        `yaml:"log_level"`
	DataDir        string        `yaml:"data_dir"`
	S3Bucket       string        `yaml:"s3_bucket"`
	S3Region       string        `yaml:"s3_region"`
	S3Prefix       string        `yaml:"s3_prefix"`
	LaunchMode     string        `yaml:"launch_mode"`
	WorkerCount    int           `yaml:"worker_count"`
	DockerImage    string        `yaml:"docker_image"`
	ECSRegion      string        `yaml:"ecs_region"`
	ECSCluster     string        `yaml:"ecs_cluster"`
	ECSTaskDef     string        `yaml:"ecs_task_def"`
	ECSSubnets     string        `yaml:"ecs_subnets"`
	ECSSG          string        `yaml:"ecs_sg"`
	Command        string        `yaml:"command"`
	Concurrency    int           `yaml:"concurrency"`
	Duration       time.Duration `yaml:"duration"`
	TotalRequests  int64         `yaml:"total_requests"`
	TargetWorkers  []string      `yaml:"target_workers"`
	Args           []string      `yaml:"args"`
	AutoStart      bool          `yaml:"auto_start"`
	// Metrics backend settings
	MetricsBackend string `yaml:"metrics_backend"` // "duckdb" (default) or "influxdb"
	InfluxDBURL    string `yaml:"influxdb_url"`
	InfluxDBToken  string `yaml:"influxdb_token"`
	InfluxDBOrg    string `yaml:"influxdb_org"`
	InfluxDBBucket string `yaml:"influxdb_bucket"`
	Platform       string `yaml:"platform"` // "local", "docker", "ecs" など
}

// DefaultConfig はデフォルトの設定を返します。
func DefaultConfig() *Config {
	return &Config{
		Port:           8080,
		ControllerAddr: "http://localhost:8080",
		LogLevel:       "info",
		DataDir:        "data",
		S3Prefix:       "swarun-metrics",
		LaunchMode:     "local",
		WorkerCount:    1,
		DockerImage:    "swarun:latest",
		Concurrency:    1,
		MetricsBackend: "duckdb",
		Platform:       "local",
	}
}

// Load はデフォルト値、YAML、および環境変数から設定を読み込みます。
// 優先順位: デフォルト値 < YAML < 環境変数
func Load(yamlBytes []byte) (*Config, error) {
	cfg := DefaultConfig()

	// 1. YAML (embed or file) があれば読み込む
	if len(yamlBytes) > 0 {
		if err := LoadYAML(cfg, yamlBytes); err != nil {
			return nil, err
		}
	}

	// 2. 環境変数で上書き (SWARUN_*)
	LoadEnv(cfg)

	// Defaults that need values after load
	if cfg.WorkerID == "" {
		if h, err := os.Hostname(); err == nil {
			cfg.WorkerID = h
		} else {
			cfg.WorkerID = "unknown-worker"
		}
	}

	return cfg, nil
}

// LoadYAML は YAML バイト列から設定を読み込み、既存の Config を上書きします。
func LoadYAML(cfg *Config, yamlBytes []byte) error {
	if err := yaml.Unmarshal(yamlBytes, cfg); err != nil {
		return fmt.Errorf("failed to unmarshal YAML config: %w", err)
	}
	return nil
}

// LoadEnv は環境変数 (SWARUN_*) から設定を読み込み、既存の Config を上書きします。
func LoadEnv(cfg *Config) {
	if v := os.Getenv("SWARUN_PORT"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			cfg.Port = i
		}
	}
	if v := os.Getenv("SWARUN_CONTROLLER_ADDR"); v != "" {
		cfg.ControllerAddr = v
	}
	if v := os.Getenv("SWARUN_WORKER_ID"); v != "" {
		cfg.WorkerID = v
	}
	if v := os.Getenv("SWARUN_LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}
	if v := os.Getenv("SWARUN_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv("SWARUN_S3_BUCKET"); v != "" {
		cfg.S3Bucket = v
	}
	if v := os.Getenv("SWARUN_S3_REGION"); v != "" {
		cfg.S3Region = v
	}
	if v := os.Getenv("SWARUN_S3_PREFIX"); v != "" {
		cfg.S3Prefix = v
	}
	if v := os.Getenv("SWARUN_LAUNCH_MODE"); v != "" {
		cfg.LaunchMode = v
	}
	if v := os.Getenv("SWARUN_WORKER_COUNT"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			cfg.WorkerCount = i
		}
	}
	if v := os.Getenv("SWARUN_DOCKER_IMAGE"); v != "" {
		cfg.DockerImage = v
	}
	if v := os.Getenv("SWARUN_ECS_REGION"); v != "" {
		cfg.ECSRegion = v
	}
	if v := os.Getenv("SWARUN_ECS_CLUSTER"); v != "" {
		cfg.ECSCluster = v
	}
	if v := os.Getenv("SWARUN_ECS_TASK_DEF"); v != "" {
		cfg.ECSTaskDef = v
	}
	if v := os.Getenv("SWARUN_ECS_SUBNETS"); v != "" {
		cfg.ECSSubnets = v
	}
	if v := os.Getenv("SWARUN_ECS_SG"); v != "" {
		cfg.ECSSG = v
	}
	if v := os.Getenv("SWARUN_COMMAND"); v != "" {
		cfg.Command = v
	}
	if v := os.Getenv("SWARUN_CONCURRENCY"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			cfg.Concurrency = i
		}
	}
	if v := os.Getenv("SWARUN_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Duration = d
		}
	}
	if v := os.Getenv("SWARUN_TOTAL_REQUESTS"); v != "" {
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.TotalRequests = i
		}
	}
	if v := os.Getenv("SWARUN_TARGET_WORKERS"); v != "" {
		cfg.TargetWorkers = strings.Split(v, ",")
	}
	if v := os.Getenv("SWARUN_ARGS"); v != "" {
		cfg.Args = strings.Split(v, ",")
	}
	if v := os.Getenv("SWARUN_AUTO_START"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.AutoStart = b
		}
	}
	if v := os.Getenv("SWARUN_METRICS_BACKEND"); v != "" {
		cfg.MetricsBackend = v
	}
	if v := os.Getenv("SWARUN_PLATFORM"); v != "" {
		cfg.Platform = v
	}
	if v := os.Getenv("SWARUN_INFLUXDB_URL"); v != "" {
		cfg.InfluxDBURL = v
	}
	if v := os.Getenv("SWARUN_INFLUXDB_TOKEN"); v != "" {
		cfg.InfluxDBToken = v
	}
	if v := os.Getenv("SWARUN_INFLUXDB_ORG"); v != "" {
		cfg.InfluxDBOrg = v
	}
	if v := os.Getenv("SWARUN_INFLUXDB_BUCKET"); v != "" {
		cfg.InfluxDBBucket = v
	}
}
