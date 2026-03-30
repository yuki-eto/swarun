package client

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/pkg/client"
	"github.com/yuki-eto/swarun/pkg/config"
	"github.com/yuki-eto/swarun/pkg/logging"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ClientArgs はクライアントモードの実行引数を保持する構造体です。
type ClientArgs struct {
	ControllerAddr  string
	Command         string
	TestID          string
	MetricName      string
	AggregateFunc   string
	AggregateWindow time.Duration
	Concurrency     int
	Duration        time.Duration
	TotalRequests   int64
	MaxDuration     time.Duration
	RampUp          time.Duration
	Stages          string // "target:duration,target:duration"
	S3Bucket        string
	S3Prefix        string
	S3Region        string
	WorkerCount     int
	DockerImage     string
	ECSCluster      string
	ECSTaskDef      string
	ECSSubnets      string
	ECSSG           string
	Labels          map[string]string
	StartTime       time.Time
	EndTime         time.Time
	WorkerID        string
}

// Run はクライアントモードを実行します。
func Run(args ClientArgs, logger *slog.Logger) {
	c := client.NewClient(args.ControllerAddr)
	ctx := context.Background()

	switch args.Command {
	case "run-test":
		var stages []*swarunv1.RampingStage
		if args.Stages != "" {
			parts := strings.Split(args.Stages, ",")
			for _, p := range parts {
				kv := strings.SplitN(p, ":", 2)
				if len(kv) == 2 {
					target, err := strconv.Atoi(kv[0])
					if err != nil {
						logger.Error("Failed to parse stage target", "stage", p, "error", err)
						os.Exit(1)
					}
					dur, err := time.ParseDuration(kv[1])
					if err != nil {
						logger.Error("Failed to parse stage duration", "stage", p, "error", err)
						os.Exit(1)
					}
					stages = append(stages, &swarunv1.RampingStage{
						Target:   int32(target),
						Duration: durationpb.New(dur),
					})
				}
			}
		}

		resp, err := c.RunTest(ctx, &swarunv1.RunTestRequest{
			TestConfig: &swarunv1.StartTestRequest{
				Concurrency:    int32(args.Concurrency),
				Duration:       durationpb.New(args.Duration),
				TotalRequests:  args.TotalRequests,
				MaxDuration:    durationpb.New(args.MaxDuration),
				RampUpDuration: durationpb.New(args.RampUp),
				Stages:         stages,
			},
		})
		if err != nil {
			logger.Error("Failed to run test", logging.ErrorAttr(err))
			os.Exit(1)
		}
		logger.Info("RunTest response", "success", resp.GetSuccess(), "message", resp.GetMessage(), "started_workers", resp.GetStartedWorkers(), "test_run_id", resp.GetTestRunId())

	case "get-status":
		// 引数から test-id を取得するか、エラーにする
		testRunID := args.TestID
		if testRunID == "" {
			logger.Error("-test-id is required for get-status")
			os.Exit(1)
		}

		resp, err := c.GetTestStatus(ctx, testRunID)
		if err != nil {
			logger.Error("Failed to get test status", logging.ErrorAttr(err))
			os.Exit(1)
		}
		logger.Info("GetTestStatus response",
			"test_run_id", resp.GetTestRunId(),
			"is_running", resp.GetIsRunning(),
			"success", resp.GetTotalSuccess(),
			"failure", resp.GetTotalFailure(),
			"avg_latency_ms", resp.GetAvgLatencyMs(),
			"max_latency_ms", resp.GetMaxLatencyMs(),
			"min_latency_ms", resp.GetMinLatencyMs(),
			"p90_latency_ms", resp.GetP90LatencyMs(),
			"p95_latency_ms", resp.GetP95LatencyMs(),
			"rps", resp.GetRps(),
			"concurrency", resp.GetConcurrency(),
			"worker_count", resp.GetWorkerCount(),
			"start_time", resp.GetStartTime().AsTime().Format(time.RFC3339),
			"end_time", resp.GetEndTime().AsTime().Format(time.RFC3339),
		)

	case "watch-status":
		// 引数から test-id を取得するか、エラーにする
		testRunID := args.TestID
		if testRunID == "" {
			logger.Error("-test-id is required for watch-status")
			os.Exit(1)
		}

		fmt.Printf("Watching test status for %s (polling every 1s)\n", testRunID)
		fmt.Printf("\n%-10s %-10s %-10s %-10s %-10s %-15s\n", "Elapsed", "VUs", "Workers", "RPS", "Success", "Avg Latency")
		fmt.Println("-----------------------------------------------------------------------")

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			resp, err := c.GetTestStatus(ctx, testRunID)
			if err != nil {
				logger.Error("Failed to get test status", logging.ErrorAttr(err))
			} else {
				msg := resp
				elapsed := time.Since(msg.GetStartTime().AsTime()).Round(time.Second)
				fmt.Printf("\r%-10s %-10d %-10d %-10.2f %-10d %-10.2f ms   ",
					elapsed,
					msg.GetConcurrency(),
					msg.GetWorkerCount(),
					msg.GetRps(),
					msg.GetTotalSuccess(),
					msg.GetAvgLatencyMs(),
				)

				if !msg.GetIsRunning() {
					fmt.Println("\n\nTest completed.")
					fmt.Println("\n==================== Test Summary ====================")
					fmt.Printf("%-20s: %s\n", "Test Run ID", msg.GetTestRunId())
					fmt.Printf("%-20s: %d\n", "Total Success", msg.GetTotalSuccess())
					fmt.Printf("%-20s: %d\n", "Total Failure", msg.GetTotalFailure())
					fmt.Printf("%-20s: %.2f ms\n", "Avg Latency", msg.GetAvgLatencyMs())
					fmt.Printf("%-20s: %.2f ms\n", "Max Latency", msg.GetMaxLatencyMs())
					fmt.Printf("%-20s: %.2f ms\n", "Min Latency", msg.GetMinLatencyMs())
					fmt.Printf("%-20s: %.2f ms\n", "90%% Latency", msg.GetP90LatencyMs())
					fmt.Printf("%-20s: %.2f ms\n", "95%% Latency", msg.GetP95LatencyMs())
					fmt.Printf("%-20s: %.2f req/s\n", "RPS", msg.GetRps())
					fmt.Printf("%-20s: %d\n", "Concurrency", msg.GetConcurrency())
					fmt.Printf("%-20s: %d\n", "Workers", msg.GetWorkerCount())
					if !msg.GetEndTime().AsTime().IsZero() {
						fmt.Printf("%-20s: %s\n", "End Time", msg.GetEndTime().AsTime().Format(time.RFC3339))
					}
					fmt.Println("======================================================")
					return
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}

	case "get-metrics":
		testRunID := args.TestID
		metricName := args.MetricName
		if testRunID == "" {
			logger.Error("-test-id is required for get-metrics")
			os.Exit(1)
		}

		req := &swarunv1.GetMetricsRequest{
			TestRunId:       testRunID,
			MetricName:      metricName,
			Labels:          args.Labels,
			AggregateFunc:   args.AggregateFunc,
			AggregateWindow: durationpb.New(args.AggregateWindow),
		}
		if !args.StartTime.IsZero() {
			req.StartTime = timestamppb.New(args.StartTime)
		}
		if !args.EndTime.IsZero() {
			req.EndTime = timestamppb.New(args.EndTime)
		}

		resp, err := c.GetMetrics(ctx, req)
		if err != nil {
			logger.Error("Failed to get metrics", logging.ErrorAttr(err))
			os.Exit(1)
		}
		logger.Info("GetMetrics response", "points_count", len(resp.Points))
		for _, p := range resp.Points {
			logger.Info("DataPoint",
				"time", p.Timestamp.AsTime().Format(time.RFC3339Nano),
				"value", p.Value,
				"labels", p.Labels,
			)
		}

	case "list-workers":
		workers, err := c.ListWorkers(ctx)
		if err != nil {
			logger.Error("Failed to list workers", logging.ErrorAttr(err))
			os.Exit(1)
		}
		logger.Info("ListWorkers response", "count", len(workers))
		for _, w := range workers {
			logger.Info("Worker",
				"id", w.GetWorkerId(),
				"hostname", w.GetHostname(),
				"address", w.GetAddress(),
				"last_heartbeat", w.GetLastHeartbeat().AsTime().Format(time.RFC3339),
			)
		}

	case "export-s3":
		testRunID := args.TestID

		resp, err := c.ExportToS3(ctx, &swarunv1.ExportToS3Request{
			S3Bucket:  args.S3Bucket,
			S3Prefix:  args.S3Prefix,
			S3Region:  args.S3Region,
			TestRunId: testRunID,
		})
		if err != nil {
			logger.Error("Failed to export to S3", logging.ErrorAttr(err))
			os.Exit(1)
		}
		logger.Info("ExportToS3 response", "success", resp.GetSuccess(), "message", resp.GetMessage())

	case "import-s3":
		testRunID := args.TestID

		resp, err := c.ImportFromS3(ctx, &swarunv1.ImportFromS3Request{
			S3Bucket:  args.S3Bucket,
			S3Prefix:  args.S3Prefix,
			S3Region:  args.S3Region,
			TestRunId: testRunID,
		})
		if err != nil {
			logger.Error("Failed to import from S3", logging.ErrorAttr(err))
			os.Exit(1)
		}
		logger.Info("ImportFromS3 response", "success", resp.GetSuccess(), "message", resp.GetMessage())

	case "provision-workers":
		cfg, err := config.Load(nil)
		if err != nil {
			logger.Error("Failed to load config", logging.ErrorAttr(err))
			os.Exit(1)
		}
		req := &swarunv1.ProvisionWorkersRequest{
			Count: int32(args.WorkerCount),
		}
		if args.ControllerAddr != "" {
			req.ControllerAddress = args.ControllerAddr
		}
		switch cfg.Platform {
		case "local":
			req.Mode = &swarunv1.ProvisionWorkersRequest_Local{
				Local: &swarunv1.LocalMode{},
			}
		case "docker":
			req.Mode = &swarunv1.ProvisionWorkersRequest_Docker{
				Docker: &swarunv1.DockerMode{Image: args.DockerImage},
			}
		case "ecs":
			req.Mode = &swarunv1.ProvisionWorkersRequest_Ecs{
				Ecs: &swarunv1.ECSMode{
					Cluster:        args.ECSCluster,
					TaskDefinition: args.ECSTaskDef,
					Region:         args.S3Region,
					Subnets:        args.ECSSubnets,
					SecurityGroups: args.ECSSG,
				},
			}
		default:
			logger.Error("Unknown platform", "platform", cfg.Platform)
			os.Exit(1)
		}

		resp, err := c.ProvisionWorkers(ctx, req)
		if err != nil {
			logger.Error("Failed to provision workers", logging.ErrorAttr(err))
			os.Exit(1)
		}
		logger.Info("ProvisionWorkers response", "success", resp.GetSuccess(), "message", resp.GetMessage(), "worker_ids", resp.GetWorkerIds())

	case "teardown-workers":
		resp, err := c.TeardownWorkers(ctx)
		if err != nil {
			logger.Error("Failed to teardown workers", logging.ErrorAttr(err))
			os.Exit(1)
		}
		logger.Info("TeardownWorkers response", "success", resp.GetSuccess(), "message", resp.GetMessage())

	case "teardown-worker":
		workerID := args.WorkerID
		if workerID == "" {
			logger.Error("-worker-id is required for teardown-worker")
			os.Exit(1)
		}
		resp, err := c.TeardownWorker(ctx, workerID)
		if err != nil {
			logger.Error("Failed to teardown worker", logging.ErrorAttr(err))
			os.Exit(1)
		}
		logger.Info("TeardownWorker response", "success", resp.GetSuccess(), "message", resp.GetMessage())

	case "export-data":
		stream, err := c.ExportData(ctx)
		if err != nil {
			logger.Error("Failed to export data", logging.ErrorAttr(err))
			os.Exit(1)
		}
		f, err := os.Create("swarun-data.zip")
		if err != nil {
			logger.Error("Failed to create local zip file", logging.ErrorAttr(err))
			os.Exit(1)
		}
		defer f.Close()

		for stream.Receive() {
			if _, err := f.Write(stream.Msg().GetChunk()); err != nil {
				logger.Error("Failed to write to local zip file", logging.ErrorAttr(err))
				os.Exit(1)
			}
		}
		if err := stream.Err(); err != nil {
			logger.Error("Stream error during export", logging.ErrorAttr(err))
			os.Exit(1)
		}
		logger.Info("Data exported successfully to swarun-data.zip")

	case "import-data":
		f, err := os.Open("swarun-data.zip")
		if err != nil {
			logger.Error("Failed to open local zip file (swarun-data.zip)", logging.ErrorAttr(err))
			os.Exit(1)
		}
		defer f.Close()

		stream := c.ImportData(ctx)
		buf := make([]byte, 64*1024)
		for {
			n, err := f.Read(buf)
			if n > 0 {
				if err := stream.Send(&swarunv1.ImportDataRequest{Chunk: buf[:n]}); err != nil {
					logger.Error("Failed to send chunk", logging.ErrorAttr(err))
					os.Exit(1)
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				logger.Error("Failed to read local zip file", logging.ErrorAttr(err))
				os.Exit(1)
			}
		}
		resp, err := stream.CloseAndReceive()
		if err != nil {
			logger.Error("Failed to receive import response", logging.ErrorAttr(err))
			os.Exit(1)
		}
		logger.Info("ImportData response", "success", resp.Msg.GetSuccess(), "message", resp.Msg.GetMessage())

	default:
		logger.Error("Unknown command", "cmd", args.Command)
		os.Exit(1)
	}
}
