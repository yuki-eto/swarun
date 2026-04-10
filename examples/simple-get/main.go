package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/yuki-eto/swarun/pkg/cli"
	"github.com/yuki-eto/swarun/pkg/config"
	"github.com/yuki-eto/swarun/pkg/logging"
	"github.com/yuki-eto/swarun/pkg/swarun"
)

func main() {
	var (
		mode        string
		concurrency int
		workerCount int
		duration    int64
		configPath  string
		runCount    int64
		rampUp      time.Duration
		stages      string
	)
	flag.StringVar(&configPath, "config", "", "Path to YAML config file")
	flag.StringVar(&mode, "mode", "controller", "Run mode: controller, worker, standalone")
	flag.IntVar(&concurrency, "concurrency", 10, "Concurrency")
	flag.IntVar(&workerCount, "worker-count", 1, "Number of workers")
	flag.Int64Var(&duration, "duration", 10, "Test duration sec")
	flag.Int64Var(&runCount, "run-count", 0, "Number of runs")
	flag.DurationVar(&rampUp, "ramp-up", 0, "Ramp up duration")
	flag.StringVar(&stages, "stages", "", "Ramp up stages (e.g., \"10:10s,20:30s\")")
	flag.Parse()

	var yamlBytes []byte
	if configPath != "" {
		var err error
		yamlBytes, err = os.ReadFile(configPath)
		if err != nil {
			slog.Error("Failed to read config file", "path", configPath, "error", err)
			os.Exit(1)
		}
	}

	cfg, err := config.Load(yamlBytes)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	if mode == "standalone" {
		cfg.AutoStart = true
		cfg.Concurrency = concurrency
		cfg.WorkerCount = workerCount
		// check port already used and change port number
		port := cfg.Port
		for {
			sock, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
			if err == nil {
				sock.Close()
				break
			}
			port++
		}
		cfg.Port = port
		if runCount > 0 {
			cfg.TotalRequests = runCount
		} else {
			cfg.Duration = time.Duration(duration) * time.Second
		}

		if os.Getenv("SWARUN_LOG_FORMAT") == "" {
			os.Setenv("SWARUN_LOG_FORMAT", "text")
		}
	}

	logger := logging.Setup(cfg.LogLevel)

	// シナリオの定義
	sc := swarun.ScenarioFunc(func(ctx context.Context, metadata string, n uint64) error {
		// HTTP GET リクエストの計測 (複数パスへのリクエスト)
		urls := []string{
			"https://httpbin.org/get",
			//"https://httpbin.org/status/200",
			//"https://httpbin.org/delay/1",
		}
		for _, url := range urls {
			resp, err := swarun.Get(url)
			if err != nil {
				return err
			}
			//b, _ := io.ReadAll(resp.Body)
			//slog.Info("Response", "url", url, "status_code", resp.StatusCode, "body", string(b))
			resp.Body.Close()
		}
		return nil
	})

	// swarun CLI の起動
	switch mode {
	case "controller":
		cli.RunController(cfg, logger)
	case "worker":
		cli.RunWorker(cfg, sc, logger)
	case "standalone":
		cli.RunStandalone(cfg, sc, rampUp, stages, logger)
	default:
		slog.Error("Unknown mode", "mode", mode)
		os.Exit(1)
	}
}
