package main

import (
	"flag"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/yuki-eto/swarun/pkg/cli/client"
	"github.com/yuki-eto/swarun/pkg/config"
	"github.com/yuki-eto/swarun/pkg/logging"
)

func main() {
	var (
		concurrency     int
		workerCount     int
		testID          string
		metricName      string
		aggregateFunc   string
		aggregateWindow time.Duration
		duration        int64
		runCount        int64
		controllerAddr  string
		command         string
		launchMode      string
		rampUp          time.Duration
		stages          string
		s3Bucket        string
		s3Prefix        string
		s3Region        string
		dockerImage     string
		ecsCluster      string
		ecsTaskDef      string
		ecsSubnets      string
		ecsSG           string
		labels          string
		startTime       string
		endTime         string
	)
	flag.IntVar(&concurrency, "concurrency", 10, "Concurrency")
	flag.IntVar(&workerCount, "worker-count", 1, "Number of workers")
	flag.StringVar(&testID, "test-id", "", "Test ID")
	flag.StringVar(&metricName, "metric-name", "", "Metric name")
	flag.StringVar(&aggregateFunc, "aggregate-func", "", "Aggregate function (mean, sum, max, min, count)")
	flag.DurationVar(&aggregateWindow, "aggregate-window", time.Second, "Aggregate window (e.g., 1s, 10s, 1m)")
	flag.Int64Var(&duration, "duration", 10, "Test duration sec")
	flag.Int64Var(&runCount, "run-count", 0, "Number of runs")
	flag.StringVar(&controllerAddr, "controller", "http://localhost:8080", "Controller address")
	flag.StringVar(&command, "cmd", "list-workers", "Client command: run-test, get-status, watch-status, get-metrics, list-workers, provision-workers, teardown-workers, export-s3, import-s3, export-data, import-data")
	flag.StringVar(&launchMode, "launch-mode", "local", "Launch mode for provision-workers: local, docker, ecs")
	flag.DurationVar(&rampUp, "ramp-up", 0, "Ramp up duration (e.g., 10s, 1m)")
	flag.StringVar(&stages, "stages", "", "Ramp up stages (e.g., \"10:10s,20:30s\")")
	flag.StringVar(&s3Bucket, "s3-bucket", "", "S3 bucket for export/import")
	flag.StringVar(&s3Prefix, "s3-prefix", "swarun-metrics", "S3 prefix for export/import")
	flag.StringVar(&s3Region, "s3-region", "ap-northeast-1", "S3 region")
	flag.StringVar(&dockerImage, "docker-image", "swarun:latest", "Docker image for provision-workers")
	flag.StringVar(&ecsCluster, "ecs-cluster", "", "ECS cluster for provision-workers")
	flag.StringVar(&ecsTaskDef, "ecs-task-def", "", "ECS task definition for provision-workers")
	flag.StringVar(&ecsSubnets, "ecs-subnets", "", "ECS subnets for provision-workers (comma separated)")
	flag.StringVar(&ecsSG, "ecs-sg", "", "ECS security groups for provision-workers (comma separated)")
	flag.StringVar(&labels, "labels", "", "Metric labels (comma separated k=v, e.g., worker_id=w1,env=prod)")
	flag.StringVar(&startTime, "start-time", "", "Start time for get-metrics (RFC3339)")
	flag.StringVar(&endTime, "end-time", "", "End time for get-metrics (RFC3339)")
	flag.Parse()

	cfg, err := config.Load(nil)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	logger := logging.Setup(cfg.LogLevel)

	// フラグがデフォルト値から変更されていない場合、cfg (環境変数やYAML) の値を使用する
	args := client.ClientArgs{
		ControllerAddr:  controllerAddr,
		Command:         command,
		TestID:          testID,
		MetricName:      metricName,
		AggregateFunc:   aggregateFunc,
		AggregateWindow: aggregateWindow,
		Concurrency:     concurrency,
		Duration:        time.Duration(duration) * time.Second,
		TotalRequests:   runCount,
		RampUp:          rampUp,
		Stages:          stages,
		S3Bucket:        s3Bucket,
		S3Prefix:        s3Prefix,
		S3Region:        s3Region,
		WorkerCount:     workerCount,
		LaunchMode:      launchMode,
		DockerImage:     dockerImage,
		ECSCluster:      ecsCluster,
		ECSTaskDef:      ecsTaskDef,
		ECSSubnets:      ecsSubnets,
		ECSSG:           ecsSG,
		Labels:          make(map[string]string),
	}

	if labels != "" {
		parts := strings.Split(labels, ",")
		for _, p := range parts {
			kv := strings.SplitN(p, "=", 2)
			if len(kv) == 2 {
				args.Labels[kv[0]] = kv[1]
			}
		}
	}

	if startTime != "" {
		t, err := time.Parse(time.RFC3339, startTime)
		if err != nil {
			slog.Error("Failed to parse start-time", "error", err)
			os.Exit(1)
		}
		args.StartTime = t
	}
	if endTime != "" {
		t, err := time.Parse(time.RFC3339, endTime)
		if err != nil {
			slog.Error("Failed to parse end-time", "error", err)
			os.Exit(1)
		}
		args.EndTime = t
	}

	// 明示的にフラグが指定されていない場合は cfg の値で上書き
	isFlagPassed := func(name string) bool {
		found := false
		flag.Visit(func(f *flag.Flag) {
			if f.Name == name {
				found = true
			}
		})
		return found
	}

	if !isFlagPassed("controller") {
		args.ControllerAddr = cfg.ControllerAddr
	}
	if !isFlagPassed("cmd") {
		args.Command = cfg.Command
	}
	if !isFlagPassed("concurrency") {
		args.Concurrency = cfg.Concurrency
	}
	if !isFlagPassed("duration") && !isFlagPassed("run-count") {
		args.Duration = cfg.Duration
		args.TotalRequests = cfg.TotalRequests
	}
	if !isFlagPassed("s3-bucket") {
		args.S3Bucket = cfg.S3Bucket
	}
	if !isFlagPassed("s3-prefix") {
		args.S3Prefix = cfg.S3Prefix
	}
	if !isFlagPassed("s3-region") {
		args.S3Region = cfg.S3Region
	}
	if !isFlagPassed("worker-count") {
		args.WorkerCount = cfg.WorkerCount
	}
	if !isFlagPassed("launch-mode") {
		args.LaunchMode = cfg.LaunchMode
	}
	if !isFlagPassed("docker-image") {
		args.DockerImage = cfg.DockerImage
	}
	if !isFlagPassed("ecs-cluster") {
		args.ECSCluster = cfg.ECSCluster
	}
	if !isFlagPassed("ecs-task-def") {
		args.ECSTaskDef = cfg.ECSTaskDef
	}
	if !isFlagPassed("ecs-subnets") {
		args.ECSSubnets = cfg.ECSSubnets
	}
	if !isFlagPassed("ecs-sg") {
		args.ECSSG = cfg.ECSSG
	}

	client.Run(args, logger)
}
