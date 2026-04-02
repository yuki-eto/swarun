package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/pkg/cli"
	"github.com/yuki-eto/swarun/pkg/client"
	"github.com/yuki-eto/swarun/pkg/config"
	"github.com/yuki-eto/swarun/pkg/logging"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Args はクライアントモードの実行引数を保持する構造体です。
type Args struct {
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
	Query           string
	QueryFormat     string
}

// Run はクライアントモードを実行します。
func Run(args Args, logger *slog.Logger) {
	c := client.NewClient(args.ControllerAddr)
	ctx := context.Background()

	switch args.Command {
	case "run-test":
		stages := cli.ParseStages(args.Stages, logger)

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
		cli.PrintTestProgressHeader()

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			resp, err := c.GetTestStatus(ctx, testRunID)
			if err != nil {
				logger.Error("Failed to get test status", logging.ErrorAttr(err))
			} else {
				cli.PrintTestProgress(resp)

				if !resp.GetIsRunning() {
					fmt.Println("\n\nTest completed.")
					cli.PrintTestSummary(resp)
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

	case "metrics-query":
		testRunID := args.TestID
		if testRunID == "" {
			logger.Error("-test-id is required for metrics-query")
			os.Exit(1)
		}
		if args.Query == "" {
			logger.Error("-query is required for metrics-query")
			os.Exit(1)
		}

		rows, err := c.QueryMetrics(ctx, testRunID, args.Query)
		if err != nil {
			logger.Error("Failed to query metrics", logging.ErrorAttr(err))
			os.Exit(1)
		}

		renderQueryResult(rows, args.QueryFormat)

	default:
		logger.Error("Unknown command", "cmd", args.Command)
		os.Exit(1)
	}
}

func renderQueryResult(rows []*swarunv1.QueryResultRow, format string) {
	if len(rows) == 0 {
		fmt.Println("No results found.")
		return
	}

	switch format {
	case "json":
		data := make([]map[string]any, 0, len(rows))
		for _, row := range rows {
			data = append(data, row.GetColumns().AsMap())
		}
		b, _ := json.MarshalIndent(data, "", "  ")
		fmt.Println(string(b))

	case "csv":
		// Get headers from first row
		cols := rows[0].GetColumns().AsMap()
		headers := sortedKeysFromMap(cols)
		fmt.Println(strings.Join(headers, ","))
		for _, row := range rows {
			m := row.GetColumns().AsMap()
			values := make([]string, 0, len(headers))
			for _, h := range headers {
				val := m[h]
				if val == nil {
					values = append(values, "")
				} else if s, ok := val.(string); ok {
					values = append(values, s)
				} else {
					// 構造体やリストなどは JSON 文字列にして CSV に入れる
					b, _ := json.Marshal(val)
					values = append(values, string(b))
				}
			}
			fmt.Println(strings.Join(values, ","))
		}

	default: // "text" or anything else
		// Calculate max widths for each column
		firstRowCols := rows[0].GetColumns().AsMap()
		headers := sortedKeysFromMap(firstRowCols)
		widths := make(map[string]int)
		for _, h := range headers {
			widths[h] = len(h)
		}

		// 全ての行を文字列化して幅を計算
		type rowData struct {
			values map[string]string
		}
		displayRows := make([]rowData, 0, len(rows))

		for _, row := range rows {
			m := row.GetColumns().AsMap()
			rd := rowData{values: make(map[string]string)}
			for h, val := range m {
				var s string
				if val == nil {
					s = ""
				} else if str, ok := val.(string); ok {
					s = str
				} else {
					b, _ := json.Marshal(val)
					s = string(b)
				}
				rd.values[h] = s
				if len(s) > widths[h] {
					widths[h] = len(s)
				}
			}
			displayRows = append(displayRows, rd)
		}

		// Render header
		for i, h := range headers {
			fmt.Printf("%-*s", widths[h]+2, h)
			if i == len(headers)-1 {
				fmt.Println()
			}
		}
		// Render separator
		for i, h := range headers {
			fmt.Print(strings.Repeat("-", widths[h]) + "  ")
			if i == len(headers)-1 {
				fmt.Println()
			}
		}
		// Render rows
		for _, rd := range displayRows {
			for i, h := range headers {
				fmt.Printf("%-*s", widths[h]+2, rd.values[h])
				if i == len(headers)-1 {
					fmt.Println()
				}
			}
		}
	}
}

func sortedKeysFromMap(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
