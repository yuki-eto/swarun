# swarun Development Guidelines

This document provides project-specific details for developers working on `swarun`.

## Build/Configuration Instructions

### Prerequisites
- Go 1.25 or later
- [Buf](https://buf.build/docs/installation) (for Protobuf generation)

### Protobuf Generation
This project uses Buf to manage Protobuf files and generate Go code. To regenerate the Connect RPC code:

```bash
buf generate
```

The generated code is stored in the `gen/` directory.

### Makefile
This project uses a `Makefile` to automate the build process.

```bash
# Generate all code and build everything
make all

# Generate Protobuf code for Go and Web
make gen-proto
make gen-web-proto

# Build the Web Dashboard
make build-web

# Build Go binaries (swarun CLI and examples)
make build-go

# Cleanup build artifacts
make clean
```

### Building the Project
You can build the `swarun` CLI or example binaries manually, but using `make` is recommended:

```bash
# General-purpose CLI (client, controller, etc.)
go build -o tmp/swarun ./cmd/swarun/main.go

# Example binary with a specific scenario
go build -o tmp/swarun-example ./examples/simple-get/main.go
```

## Testing Information

### Running Tests
Standard Go testing tools are used. To run all tests in the project:

```bash
go test ./...
```

To run tests in a specific package with verbose output:

```bash
go test -v ./pkg/config
```

### Test Maintenance
**Important**: All test code created during development must be preserved in the repository. Do not delete reproduction scripts or unit tests after the fix is verified. This ensures long-term reliability and prevents regressions.

### Adding New Tests
When adding new features or fixing bugs, follow these guidelines:
- Place tests in the same package as the code being tested, using the `_test.go` suffix.
- Use `t.Setenv` for testing configuration that depends on environment variables.
- For Connect RPC-related tests, mock the `ControllerService` or `WorkerService` as needed.

### Simple Test Example
The following example demonstrates how to test the configuration loading logic in `pkg/config`:

```go
package config

import (
	"testing"
	"time"
)

func TestLoadEnv(t *testing.T) {
	// Setup environment variables for the test
	t.Setenv("SWARUN_PORT", "9090")
	t.Setenv("SWARUN_DURATION", "5m")
	
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
}
```

## Additional Development Information

### Code Style and Conventions
- **Comments**: This project uses Japanese for documentation comments within the source code. Please maintain this convention for consistency.
- **Idioms**: This project follows modern Go idioms (Go 1.25+). Use `any` instead of `interface{}`, `errors.Is`/`errors.As` for error handling, `time.Since` for duration calculations, and `for i := range n` for loops.
- **Persistence**: Test run IDs are persisted in `runs.json` within the `SWARUN_DATA_DIR`. This allows the controller to remember past test runs across restarts.
- **Real-time Monitoring**: Use the `watch-status` command to monitor test progress in real-time with in-place updates.
- **Advanced Metrics Query**: The `get-metrics` command supports filtering by labels, time ranges, and server-side aggregation (for InfluxDB backend).
- **Project Structure**:
    - `cmd/swarun`: General-purpose CLI for sending commands to the controller.
    - `examples/`: Example scenarios that can be run as controller, worker, or standalone.
    - `internal/`: Private implementation details.
        - `dao/`: Data Access Objects for metrics storage (tstorage, influxdb).
    - `pkg/`: Publicly accessible packages.
        - `cli/`: Server-side CLI logic.
        - `cli/client/`: Client-side CLI logic for `swarun` command.
        - `client/`: Go client library for interacting with the controller via Connect RPC.
    - `proto/`: Protobuf definitions.
    - `gen/`: Generated code from Protobuf.
- **Configuration**: `swarun` uses a hierarchical configuration system:
    1. Command-line flags (highest priority)
    2. Environment variables (`SWARUN_*`)
    3. YAML configuration file
    4. Default values (lowest priority)

### Metrics Backend Configuration
You can choose the backend for metrics storage. The default is `duckdb` (embedded).

#### DuckDB (Default)
Metrics are stored in the local directory specified by `SWARUN_DATA_DIR`.

#### InfluxDB
To use InfluxDB as a backend, set the following environment variables:
- `SWARUN_METRICS_BACKEND`: `influxdb`
- `SWARUN_INFLUXDB_URL`: InfluxDB server URL (e.g., `http://localhost:8086`)
- `SWARUN_INFLUXDB_TOKEN`: InfluxDB authentication token
- `SWARUN_INFLUXDB_ORG`: InfluxDB organization name
- `SWARUN_INFLUXDB_BUCKET`: InfluxDB bucket name

### Docker Compose (Local Distributed Testing)
You can run a full distributed setup (Controller, Worker, and InfluxDB) locally using Docker Compose:

```bash
docker-compose up --build
```

This will start:
- **InfluxDB**: Metrics storage (port 8086)
- **Controller**: Manages workers and aggregates metrics (port 8080)
- **Worker**: Executes test scenarios

Once the services are up, you can start and monitor tests using the `swarun` CLI.

```bash
# Build the tool
go build -o tmp/swarun ./cmd/swarun/main.go

# Start a test
./tmp/swarun -cmd run-test -concurrency 5 -duration 30s

# Monitor in real-time
./tmp/swarun -cmd watch-status -test-id <UUID>

# Get aggregated metrics (InfluxDB only)
./tmp/swarun -cmd get-metrics -test-id <UUID> -metric-name latency_ms -aggregate-func mean -aggregate-window 10s
```

You can also run the CLI inside a Docker container:

```bash
docker-compose exec controller swarun -cmd list-workers
```

### Metrics Recording
When writing a custom `Scenario`, use the `swarun` package wrappers to automatically record metrics:

```go
sc := swarun.ScenarioFunc(func(ctx context.Context) error {
    // Automatically records latency, success/failure, and response size
    resp, err := swarun.Get("http://example.com")
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    return nil
})
```

- **Dashboard**: A React-based web dashboard is integrated into the controller. Access it at `http://localhost:8080` (default port).
- **Ramp-up and Stages**: `swarun` supports flexible ramp-up strategies specified at test execution time. These are not part of the persistent configuration (YAML/Env).

- **Simple Ramp-up**: Use the `-ramp-up` flag to specify a duration to reach the target concurrency linearly.
- **Stages**: Use the `-stages` flag for complex ramping (similar to k6). Format: `"target:duration,target:duration"`.
  - Example: `./tmp/swarun -cmd run-test -stages "10:30s,20:1m,20:2m"`
  - This example starts with 0 and reaches 10 VUs in 30s, then ramps to 20 VUs over the next 1m, and stays at 20 VUs for 2m.
