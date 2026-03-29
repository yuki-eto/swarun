package config

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Port != 8080 {
		t.Errorf("expected port 8080, got %d", cfg.Port)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("expected LogLevel info, got %s", cfg.LogLevel)
	}
	if cfg.MetricsBackend != "duckdb" {
		t.Errorf("expected MetricsBackend duckdb, got %s", cfg.MetricsBackend)
	}
	if cfg.Platform != "local" {
		t.Errorf("expected Platform local, got %s", cfg.Platform)
	}
}

func TestLoadEnv(t *testing.T) {
	// Setup environment variables for the test
	t.Setenv("SWARUN_PORT", "9090")
	t.Setenv("SWARUN_DURATION", "5m")
	t.Setenv("SWARUN_METRICS_BACKEND", "influxdb")
	t.Setenv("SWARUN_PLATFORM", "docker")
	t.Setenv("SWARUN_ECS_REGION", "ap-northeast-1")

	// Load configuration (it should prioritize environment variables)
	cfg, err := Load(nil)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Verify the values
	if cfg.Port != 9090 {
		t.Errorf("expected port 9090 from env, got %d", cfg.Port)
	}
	if cfg.Duration != 5*time.Minute {
		t.Errorf("expected duration 5m from env, got %v", cfg.Duration)
	}
	if cfg.MetricsBackend != "influxdb" {
		t.Errorf("expected backend influxdb from env, got %s", cfg.MetricsBackend)
	}
	if cfg.Platform != "docker" {
		t.Errorf("expected platform docker from env, got %s", cfg.Platform)
	}
	if cfg.ECSRegion != "ap-northeast-1" {
		t.Errorf("expected ecs_region ap-northeast-1 from env, got %s", cfg.ECSRegion)
	}
}

func TestLoadYAML(t *testing.T) {
	yamlContent := `
port: 7070
controller_addr: http://localhost:7070
log_level: debug
concurrency: 20
duration: 1m
`
	cfg, err := Load([]byte(yamlContent))
	if err != nil {
		t.Fatalf("failed to load YAML config: %v", err)
	}

	if cfg.Port != 7070 {
		t.Errorf("expected port 7070, got %d", cfg.Port)
	}
	if cfg.ControllerAddr != "http://localhost:7070" {
		t.Errorf("expected controller_addr http://localhost:7070, got %s", cfg.ControllerAddr)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected LogLevel debug, got %s", cfg.LogLevel)
	}
	if cfg.Concurrency != 20 {
		t.Errorf("expected concurrency 20, got %d", cfg.Concurrency)
	}
	if cfg.Duration != 1*time.Minute {
		t.Errorf("expected duration 1m, got %v", cfg.Duration)
	}
}
