package controller

import (
	"archive/zip"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/gen/proto/v1/swarunv1connect"
	"github.com/yuki-eto/swarun/internal/atomicmap"
	"github.com/yuki-eto/swarun/internal/dao"
	"github.com/yuki-eto/swarun/internal/orchestrator"
	"github.com/yuki-eto/swarun/pkg/config"
	"github.com/yuki-eto/swarun/pkg/logging"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Controller struct {
	workers         *atomicmap.Map[string, *Worker]
	storages        *atomicmap.Map[string, dao.MetricsDAO]
	testRuns        *atomicmap.Map[string, *TestRun]
	orchestrator    *orchestrator.Orchestrator
	logger          *slog.Logger
	cfg             *config.Config
	dataDir         string
	defaultS3Bucket string
	defaultS3Region string
	defaultS3Prefix string
}

func NewController(logger *slog.Logger, cfg *config.Config) (*Controller, error) {
	if logger == nil {
		logger = slog.Default()
	}
	c := &Controller{
		workers:         atomicmap.New[string, *Worker](),
		storages:        atomicmap.New[string, dao.MetricsDAO](),
		testRuns:        atomicmap.New[string, *TestRun](),
		orchestrator:    orchestrator.NewOrchestrator(logger, cfg),
		logger:          logger,
		cfg:             cfg,
		dataDir:         cfg.DataDir,
		defaultS3Bucket: cfg.S3Bucket,
		defaultS3Region: cfg.S3Region,
		defaultS3Prefix: cfg.S3Prefix,
	}

	if err := c.loadTestRuns(); err != nil {
		logger.Warn("Failed to load test runs", "error", err)
	}

	return c, nil
}

func (c *Controller) loadTestRuns() error {
	path := filepath.Join(c.dataDir, "runs.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var runs []*TestRun
	if err := json.Unmarshal(data, &runs); err != nil {
		// 旧形式 (IDの配列) かもしれないので試みる
		var ids []string
		if err2 := json.Unmarshal(data, &ids); err2 == nil {
			for _, id := range ids {
				tr := &TestRun{
					ID:          id,
					IsRunning:   false,
					PathMetrics: NewPathMetricsMap(),
				}
				c.testRuns.Store(id, tr)
				c.restoreStatsFromStorage(id, tr)
			}
			return nil
		}
		return err
	}

	// 読み込んだ情報をマップに格納
	for _, tr := range runs {
		if tr.PathMetrics == nil {
			tr.PathMetrics = NewPathMetricsMap()
		}
		// 再起動時は強制的に Running 状態を false にする
		tr.IsRunning = false
		c.testRuns.Store(tr.ID, tr)

		// メトリクスが必要な場合はストレージから復元
		// (Latencies など JSON に保存していないもの)
		c.restoreStatsFromStorage(tr.ID, tr)
	}
	return nil
}

func (c *Controller) restoreStatsFromStorage(testRunID string, tr *TestRun) {
	storage, err := c.getStorage(testRunID)
	if err != nil {
		return
	}

	ctx := context.Background()
	// 全期間のサマリーを取得
	// DuckDB なら SQL で一発で取得できるが、インターフェースが SelectRows なので
	// 大きなウィンドウで集計を投げる

	// 100年分くらいのウィンドウ
	longWindow := 100 * 365 * 24 * time.Hour

	// 成功数
	successRows, _ := storage.SelectRows(ctx, "success", nil, time.Time{}, time.Now(), "sum", longWindow)
	if len(successRows) > 0 {
		// ここでは全合計が取れてしまうので、後で SelectStats の結果で上書きする
	}

	// 統計情報の復元
	overall, pathStats, err := storage.SelectStats(ctx, nil, time.Time{}, time.Now())
	if err == nil && len(overall) > 0 {
		tr.TotalSuccess = 0
		tr.TotalFailure = 0
		tr.TotalLatency = 0
		tr.LatencyCount = 0
		tr.TotalIterations = 0

		for path, stats := range pathStats {
			if tr.PathMetrics == nil {
				tr.PathMetrics = NewPathMetricsMap()
			}
			tr.PathMetrics.mu.Lock()
			tr.PathMetrics.Metrics[path] = &PathStats{
				Success:      int64(stats["success"]),
				Failure:      int64(stats["failure"]),
				TotalLatency: time.Duration(stats["avg_latency"] * (stats["success"] + stats["failure"]) * float64(time.Millisecond)),
				MinLatencyMs: stats["min_latency"],
				MaxLatencyMs: stats["max_latency"],
			}
			tr.PathMetrics.mu.Unlock()

			if path == "scenario_iteration" {
				tr.TotalIterations = int64(stats["success"])
			} else {
				tr.TotalSuccess += int64(stats["success"])
				tr.TotalFailure += int64(stats["failure"])
				tr.TotalLatency += time.Duration(stats["avg_latency"] * (stats["success"] + stats["failure"]) * float64(time.Millisecond))
				tr.LatencyCount += int64(stats["success"] + stats["failure"])
				if tr.MaxLatencyMs < stats["max_latency"] {
					tr.MaxLatencyMs = stats["max_latency"]
				}
				if tr.MinLatencyMs == 0 || tr.MinLatencyMs > stats["min_latency"] {
					tr.MinLatencyMs = stats["min_latency"]
				}
			}
		}
	}

	// パーセンタイル計算のために生データが必要な場合は、ここでも復元する
	// ただし、非常に大量のデータになる可能性があるため注意
	latencyRows, _ := storage.SelectRows(ctx, "latency_ms", nil, time.Time{}, time.Now(), "", 0)
	if len(latencyRows) > 0 {
		tr.LatenciesMu.Lock()
		for _, row := range latencyRows {
			tr.Latencies = append(tr.Latencies, row.Value)
		}
		tr.LatenciesMu.Unlock()
	}
}

func (c *Controller) saveTestRuns() error {
	if err := os.MkdirAll(c.dataDir, 0755); err != nil {
		return err
	}

	runs := slices.Collect(maps.Values(c.testRuns.Load()))
	slices.SortFunc(runs, func(a, b *TestRun) int {
		return cmp.Compare(a.ID, b.ID)
	})

	data, err := json.MarshalIndent(runs, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(c.dataDir, "runs.json"), data, 0644)
}

// Close は全ストレージのリソースをクリーンアップし、オーケストレーターのプロセスをすべて閉じます。
// また、終了時に data_dir の内容を S3 にエクスポートします。
func (c *Controller) Close() error {
	var errs []string

	// S3 へのエクスポート処理 (エラーが発生しても続行)
	if c.defaultS3Bucket != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, err := c.ExportToS3(ctx, connect.NewRequest(&swarunv1.ExportToS3Request{
			S3Bucket: c.defaultS3Bucket,
			S3Prefix: c.defaultS3Prefix,
			S3Region: c.defaultS3Region,
		}))
		cancel()
		if err != nil {
			errs = append(errs, fmt.Sprintf("failed to export data to S3 on close: %v", err))
		}
	}

	// オーケストレーターの終了処理
	if err := c.orchestrator.Teardown(context.Background()); err != nil {
		errs = append(errs, fmt.Sprintf("failed to teardown orchestrator: %v", err))
	}

	// ストレージの終了処理
	for _, s := range c.storages.Load() {
		if err := s.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("failed to close storage: %v", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (c *Controller) RegisterWorker(
	ctx context.Context,
	req *connect.Request[swarunv1.RegisterWorkerRequest],
) (*connect.Response[swarunv1.RegisterWorkerResponse], error) {
	id := req.Msg.GetWorkerId()
	hostname := req.Msg.GetHostname()
	address := req.Msg.GetAddress()

	c.workers.Store(id, &Worker{
		ID:            id,
		Hostname:      hostname,
		Address:       address,
		LastHeartbeat: time.Now(),
	})

	c.logger.Info("Worker registered", "id", id, "hostname", hostname, "address", address)

	return connect.NewResponse(&swarunv1.RegisterWorkerResponse{
		Success: true,
		Message: "Registered successfully",
	}), nil
}

func (c *Controller) RunTest(
	ctx context.Context,
	req *connect.Request[swarunv1.RunTestRequest],
) (*connect.Response[swarunv1.RunTestResponse], error) {
	testConfig := req.Msg.GetTestConfig()

	// テストIDが指定されていない、または "random" の場合はランダムな文字列を生成
	testRunID := testConfig.GetTestRunId()
	if testRunID == "" || testRunID == "random" {
		testRunID = uuid.New().String()
		testConfig.TestRunId = testRunID
	}

	targetWorkerIDs := req.Msg.GetWorkerIds()

	allWorkers := c.workers.Load()
	var workersToStart []*Worker

	if len(targetWorkerIDs) > 0 {
		for _, id := range targetWorkerIDs {
			if w, ok := allWorkers[id]; ok {
				workersToStart = append(workersToStart, w)
			} else {
				c.logger.Warn("Target worker not found", "worker_id", id)
			}
		}
	} else {
		for _, w := range allWorkers {
			workersToStart = append(workersToStart, w)
		}
	}

	if len(workersToStart) == 0 {
		return connect.NewResponse(&swarunv1.RunTestResponse{
			Success: false,
			Message: "No workers available to start test",
		}), nil
	}

	startedWorkers := make([]string, 0, len(workersToStart))
	var lastErr error

	for _, w := range workersToStart {
		if w.Address == "" {
			c.logger.Warn("Worker address is empty, skipping", "worker_id", w.ID)
			continue
		}

		client := swarunv1connect.NewWorkerServiceClient(
			http.DefaultClient,
			w.Address,
		)

		_, err := client.StartTest(ctx, connect.NewRequest(testConfig))
		if err != nil {
			c.logger.Error("Failed to start test on worker", "worker_id", w.ID, "address", w.Address, logging.ErrorAttr(err))
			lastErr = err
			continue
		}

		c.logger.Info("Started test on worker", "worker_id", w.ID, "test_run_id", testConfig.GetTestRunId())
		startedWorkers = append(startedWorkers, w.ID)
	}

	message := fmt.Sprintf("Started test on %d workers", len(startedWorkers))
	if lastErr != nil {
		message += " (some errors occurred)"
	}

	// テスト実行の進捗管理を開始
	c.testRuns.Store(testRunID, &TestRun{
		ID:                 testRunID,
		StartTime:          time.Now(),
		ConfiguredDuration: testConfig.GetDuration().AsDuration(),
		Concurrency:        testConfig.GetConcurrency(),
		IsRunning:          true,
		WorkerCount:        int32(len(startedWorkers)),
		MinLatencyMs:       0,
		MaxLatencyMs:       0,
		PathMetrics:        NewPathMetricsMap(),
	})

	if err := c.saveTestRuns(); err != nil {
		c.logger.Error("Failed to save test runs", "error", err)
	}

	return connect.NewResponse(&swarunv1.RunTestResponse{
		Success:        len(startedWorkers) > 0,
		Message:        message,
		StartedWorkers: startedWorkers,
		TestRunId:      testRunID,
	}), nil
}

func (c *Controller) GetTestStatus(
	ctx context.Context,
	req *connect.Request[swarunv1.GetTestStatusRequest],
) (*connect.Response[swarunv1.GetTestStatusResponse], error) {
	testRunID := req.Msg.GetTestRunId()
	tr, ok := c.testRuns.Get(testRunID)
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("test run not found: %s", testRunID))
	}

	avgLatency := 0.0
	p90 := 0.0
	p95 := 0.0
	rps := 0.0
	pathMetrics := make(map[string]*swarunv1.PathMetrics)

	duration := time.Since(tr.StartTime)
	if !tr.EndTime.IsZero() {
		duration = tr.EndTime.Sub(tr.StartTime)
	}

	// テストが終了している場合はストレージから再集計（DuckDB のみ対応）
	if !tr.IsRunning {
		storage, err := c.getStorage(testRunID)
		if err == nil {
			_, pathStats, err := storage.SelectStats(ctx, nil, tr.StartTime, tr.EndTime)
			if err == nil {
				tr.TotalSuccess = 0
				tr.TotalFailure = 0
				tr.TotalIterations = 0
				var totalLatencyMs float64
				var latencyCount int64

				for path, stats := range pathStats {
					pm := &swarunv1.PathMetrics{
						TotalSuccess: int64(stats["success"]),
						TotalFailure: int64(stats["failure"]),
						AvgLatencyMs: stats["avg_latency"],
						MaxLatencyMs: stats["max_latency"],
						MinLatencyMs: stats["min_latency"],
					}
					if duration > 0 {
						pm.Rps = float64(pm.TotalSuccess+pm.TotalFailure) / duration.Seconds()
					}

					// DuckDB からパスごとの平均データも取れるように
					latencyRows, _ := storage.SelectRows(ctx, "latency_ms", map[string]string{"path": path}, tr.StartTime, tr.EndTime, "", 0)
					if len(latencyRows) > 0 {
						lats := make([]float64, 0, len(latencyRows))
						for _, r := range latencyRows {
							lats = append(lats, r.Value)
						}
						slices.SortFunc(lats, cmp.Compare[float64])
						idx90 := int(float64(len(lats))*0.9+0.99) - 1
						if idx90 < 0 {
							idx90 = 0
						} else if idx90 >= len(lats) {
							idx90 = len(lats) - 1
						}
						pm.P90LatencyMs = lats[idx90]

						idx95 := int(float64(len(lats))*0.95+0.99) - 1
						if idx95 < 0 {
							idx95 = 0
						} else if idx95 >= len(lats) {
							idx95 = len(lats) - 1
						}
						pm.P95LatencyMs = lats[idx95]
					}
					pathMetrics[path] = pm

					if path == "scenario_iteration" {
						tr.TotalIterations = pm.TotalSuccess
					} else {
						tr.TotalSuccess += pm.TotalSuccess
						tr.TotalFailure += pm.TotalFailure
						totalLatencyMs += stats["avg_latency"] * float64(pm.TotalSuccess+pm.TotalFailure)
						latencyCount += (pm.TotalSuccess + pm.TotalFailure)
						if tr.MaxLatencyMs < pm.MaxLatencyMs {
							tr.MaxLatencyMs = pm.MaxLatencyMs
						}
						if tr.MinLatencyMs == 0 || tr.MinLatencyMs > pm.MinLatencyMs {
							tr.MinLatencyMs = pm.MinLatencyMs
						}
					}

					// インメモリのキャッシュも更新
					if tr.PathMetrics != nil {
						tr.PathMetrics.mu.Lock()
						if s, ok := tr.PathMetrics.Metrics[path]; ok {
							s.Success = pm.TotalSuccess
							s.Failure = pm.TotalFailure
							s.MinLatencyMs = pm.MinLatencyMs
							s.MaxLatencyMs = pm.MaxLatencyMs
							s.P90LatencyMs = pm.P90LatencyMs
							s.P95LatencyMs = pm.P95LatencyMs
						}
						tr.PathMetrics.mu.Unlock()
					}
				}
				if latencyCount > 0 {
					avgLatency = totalLatencyMs / float64(latencyCount)
				}
			}
		}
	} else {
		// 実行中の場合はインメモリの統計を使用
		if tr.TotalSuccess > 0 || tr.TotalFailure > 0 {
			if tr.LatencyCount > 0 {
				avgLatency = float64(tr.TotalLatency.Milliseconds()) / float64(tr.LatencyCount)
			}
		}
	}

	if avgLatency == 0 && tr.LatencyCount > 0 {
		avgLatency = float64(tr.TotalLatency.Milliseconds()) / float64(tr.LatencyCount)
	}

	if duration > 0 {
		rps = float64(tr.TotalSuccess+tr.TotalFailure) / duration.Seconds()
	}

	// パーセンタイル計算 (これだけは生データが必要)
	tr.LatenciesMu.RLock()
	if len(tr.Latencies) > 0 {
		lats := slices.Clone(tr.Latencies)
		tr.LatenciesMu.RUnlock()

		slices.SortFunc(lats, cmp.Compare[float64])
		// 最近傍法 (Nearest Rank) によるインデックス計算
		// index = ceil(p * N) - 1
		idx90 := int(float64(len(lats))*0.9+0.99) - 1
		if idx90 < 0 {
			idx90 = 0
		} else if idx90 >= len(lats) {
			idx90 = len(lats) - 1
		}
		p90 = lats[idx90]

		idx95 := int(float64(len(lats))*0.95+0.99) - 1
		if idx95 < 0 {
			idx95 = 0
		} else if idx95 >= len(lats) {
			idx95 = len(lats) - 1
		}
		p95 = lats[idx95]
	} else {
		tr.LatenciesMu.RUnlock()
	}

	// パスごとの統計を集計 (インメモリの結果とマージ、または上書き)
	if tr.PathMetrics != nil {
		tr.PathMetrics.mu.RLock()
		for path, stats := range tr.PathMetrics.Metrics {
			if _, ok := pathMetrics[path]; ok {
				// すでにストレージから取得済みの場合はスキップ
				continue
			}
			pm := &swarunv1.PathMetrics{
				TotalSuccess: stats.Success,
				TotalFailure: stats.Failure,
			}
			if len(stats.Latencies) > 0 {
				lats := slices.Clone(stats.Latencies)
				slices.SortFunc(lats, cmp.Compare[float64])
				sum := 0.0
				for _, v := range lats {
					sum += v
				}
				pm.AvgLatencyMs = sum / float64(len(lats))
				pm.MinLatencyMs = lats[0]
				pm.MaxLatencyMs = lats[len(lats)-1]

				// p90, p95
				idx90 := int(float64(len(lats))*0.9+0.99) - 1
				if idx90 < 0 {
					idx90 = 0
				} else if idx90 >= len(lats) {
					idx90 = len(lats) - 1
				}
				pm.P90LatencyMs = lats[idx90]

				idx95 := int(float64(len(lats))*0.95+0.99) - 1
				if idx95 < 0 {
					idx95 = 0
				} else if idx95 >= len(lats) {
					idx95 = len(lats) - 1
				}
				pm.P95LatencyMs = lats[idx95]

				// キャッシュを更新しておく
				stats.MinLatencyMs = pm.MinLatencyMs
				stats.MaxLatencyMs = pm.MaxLatencyMs
				stats.P90LatencyMs = pm.P90LatencyMs
				stats.P95LatencyMs = pm.P95LatencyMs
			} else if stats.TotalLatency > 0 && (stats.Success+stats.Failure) > 0 {
				// DuckDB からロードされたデータなど、Latencies スライスがない場合のフォールバック
				pm.AvgLatencyMs = float64(stats.TotalLatency.Milliseconds()) / float64(stats.Success+stats.Failure)
				pm.MinLatencyMs = stats.MinLatencyMs
				pm.MaxLatencyMs = stats.MaxLatencyMs
				pm.P90LatencyMs = stats.P90LatencyMs
				pm.P95LatencyMs = stats.P95LatencyMs
			}

			// RPS
			if duration > 0 {
				pm.Rps = float64(pm.TotalSuccess+pm.TotalFailure) / duration.Seconds()
			}
			pathMetrics[path] = pm
		}
		tr.PathMetrics.mu.RUnlock()
	}

	res := &swarunv1.GetTestStatusResponse{
		TestRunId:       tr.ID,
		IsRunning:       tr.IsRunning,
		TotalSuccess:    tr.TotalSuccess,
		TotalFailure:    tr.TotalFailure,
		AvgLatencyMs:    avgLatency,
		WorkerCount:     tr.WorkerCount,
		StartTime:       timestamppb.New(tr.StartTime),
		MaxLatencyMs:    tr.MaxLatencyMs,
		MinLatencyMs:    tr.MinLatencyMs,
		P90LatencyMs:    p90,
		P95LatencyMs:    p95,
		Rps:             rps,
		EndTime:         timestamppb.New(tr.EndTime),
		PathMetrics:     pathMetrics,
		Duration:        durationpb.New(tr.ConfiguredDuration),
		Concurrency:     tr.Concurrency,
		TotalIterations: tr.TotalIterations,
	}
	return connect.NewResponse(res), nil
}

func (c *Controller) ProvisionWorkers(
	ctx context.Context,
	req *connect.Request[swarunv1.ProvisionWorkersRequest],
) (*connect.Response[swarunv1.ProvisionWorkersResponse], error) {
	ids, err := c.orchestrator.Provision(ctx, req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&swarunv1.ProvisionWorkersResponse{
		Success:   true,
		Message:   fmt.Sprintf("Successfully provisioned %d workers", len(ids)),
		WorkerIds: ids,
	}), nil
}

func (c *Controller) TeardownWorkers(
	ctx context.Context,
	_ *connect.Request[swarunv1.TeardownWorkersRequest],
) (*connect.Response[swarunv1.TeardownWorkersResponse], error) {
	if err := c.orchestrator.Teardown(ctx); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&swarunv1.TeardownWorkersResponse{
		Success: true,
		Message: "Successfully teared down all workers",
	}), nil
}

func (c *Controller) StopTest(
	ctx context.Context,
	req *connect.Request[swarunv1.StopTestRequest],
) (*connect.Response[swarunv1.StopTestResponse], error) {
	testRunID := req.Msg.GetTestRunId()
	tr, ok := c.testRuns.Get(testRunID)
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("test run not found: %s", testRunID))
	}

	if !tr.IsRunning {
		return connect.NewResponse(&swarunv1.StopTestResponse{
			Stopped: true,
		}), nil
	}

	// 全ワーカーに対して停止命令を送信
	allWorkers := c.workers.Load()
	for _, w := range allWorkers {
		if w.Address == "" {
			continue
		}
		client := swarunv1connect.NewWorkerServiceClient(
			http.DefaultClient,
			w.Address,
		)
		_, err := client.StopTest(ctx, connect.NewRequest(&swarunv1.StopTestRequest{
			TestRunId: testRunID,
		}))
		if err != nil {
			c.logger.Warn("Failed to stop test on worker", "worker_id", w.ID, "error", err)
		}
	}

	// ステータスを更新
	tr.IsRunning = false
	tr.EndTime = time.Now()

	if err := c.saveTestRuns(); err != nil {
		c.logger.Error("Failed to save test runs on stop", "error", err)
	}

	return connect.NewResponse(&swarunv1.StopTestResponse{
		Stopped: true,
	}), nil
}

func (c *Controller) ListTestRuns(
	ctx context.Context,
	_ *connect.Request[swarunv1.ListTestRunsRequest],
) (*connect.Response[swarunv1.ListTestRunsResponse], error) {
	ids := slices.Collect(maps.Keys(c.testRuns.Load()))
	slices.Sort(ids)
	slices.Reverse(ids) // 新しい順に表示

	return connect.NewResponse(&swarunv1.ListTestRunsResponse{
		TestRunIds: ids,
	}), nil
}

func (c *Controller) ExportReport(
	ctx context.Context,
	req *connect.Request[swarunv1.ExportReportRequest],
) (*connect.Response[swarunv1.ExportReportResponse], error) {
	testRunID := req.Msg.GetTestRunId()
	if _, ok := c.testRuns.Get(testRunID); !ok {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("test run not found: %s", testRunID))
	}

	// レポート用のデータを収集
	statusResp, err := c.GetTestStatus(ctx, connect.NewRequest(&swarunv1.GetTestStatusRequest{TestRunId: testRunID}))
	if err != nil {
		return nil, err
	}
	status := statusResp.Msg

	// メトリクスを取得 (グラフ用)
	latencyResp, err := c.GetMetrics(ctx, connect.NewRequest(&swarunv1.GetMetricsRequest{
		TestRunId:       testRunID,
		MetricName:      "latency_ms",
		AggregateFunc:   "mean",
		AggregateWindow: durationpb.New(5 * time.Second),
	}))
	if err != nil {
		return nil, err
	}

	rpsResp, err := c.GetMetrics(ctx, connect.NewRequest(&swarunv1.GetMetricsRequest{
		TestRunId:       testRunID,
		MetricName:      "success",
		AggregateFunc:   "sum",
		AggregateWindow: durationpb.New(5 * time.Second),
	}))
	if err != nil {
		return nil, err
	}

	// HTMLを生成
	html, err := c.generateHTMLReport(status, latencyResp.Msg.GetPoints(), rpsResp.Msg.GetPoints())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to generate report: %w", err))
	}

	return connect.NewResponse(&swarunv1.ExportReportResponse{
		Html: html,
	}), nil
}

type MetricStream interface {
	Receive() bool
	Msg() *swarunv1.MetricBatch
	Err() error
}

func (c *Controller) SendMetrics(
	ctx context.Context,
	stream *connect.ClientStream[swarunv1.MetricBatch],
) (*connect.Response[swarunv1.SendMetricsResponse], error) {
	return c.sendMetrics(ctx, stream)
}

func (c *Controller) sendMetrics(
	ctx context.Context,
	stream MetricStream,
) (*connect.Response[swarunv1.SendMetricsResponse], error) {
	for stream.Receive() {
		batch := stream.Msg()
		testRunID := batch.GetTestRunId()
		storage, err := c.getStorage(testRunID)
		if err != nil || storage == nil {
			c.logger.Error("Failed to get storage for test run", "test_run_id", testRunID)
			continue
		}

		rows := make([]dao.Row, 0, len(batch.GetMetrics()))
		ts := batch.GetTimestamp().AsTime()
		if ts.IsZero() {
			ts = time.Now()
		}

		for _, m := range batch.GetMetrics() {
			// 進捗状況を更新
			if tr, ok := c.testRuns.Get(testRunID); ok {
				// パスごとのメトリクスを集計 (インメモリ)
				path := m.GetLabels()["path"]
				if path == "" {
					path = "unknown"
				}

				if tr.PathMetrics == nil {
					tr.PathMetrics = NewPathMetricsMap()
				}
				tr.PathMetrics.Add(path, m.GetName(), m.GetValue())

				if path == "scenario_iteration" {
					// 案Bにより、シナリオ単位の success/failure/latency_ms は送信されなくなったが、
					// 過去のデータの互換性や、他のメトリクスがある可能性を考慮して集計処理は残す。
					// ただし、現在は worker からは test_finished しか送られないはず。
					if m.GetName() == "success" {
						atomic.AddInt64(&tr.TotalIterations, int64(m.GetValue()))
					}
					// scenario_iteration のメトリクスは、全体の統計（Success/Failure/Latency）には含めない
				} else {
					switch m.GetName() {
					case "success":
						atomic.AddInt64(&tr.TotalSuccess, int64(m.GetValue()))
					case "failure":
						atomic.AddInt64(&tr.TotalFailure, int64(m.GetValue()))
					case "latency_ms":
						val := m.GetValue()
						atomic.AddInt64((*int64)(&tr.TotalLatency), int64(val*float64(time.Millisecond)))
						atomic.AddInt64(&tr.LatencyCount, 1)

						tr.LatenciesMu.Lock()
						tr.Latencies = append(tr.Latencies, val)
						if tr.MaxLatencyMs < val {
							tr.MaxLatencyMs = val
						}
						if tr.MinLatencyMs == 0 || tr.MinLatencyMs > val {
							tr.MinLatencyMs = val
						}
						tr.LatenciesMu.Unlock()
					}
				}

				if m.GetName() == "test_finished" {
					finishedCount := atomic.AddInt32(&tr.FinishedWorkerCount, 1)
					if finishedCount >= tr.WorkerCount {
						tr.IsRunning = false
						tr.EndTime = time.Now()
						c.logger.Info("Test run finished", "test_run_id", testRunID, "success", tr.TotalSuccess, "failure", tr.TotalFailure, "end_time", tr.EndTime)
						if err := c.saveTestRuns(); err != nil {
							c.logger.Error("Failed to save test runs on finish", "error", err)
						}
					}
				}
			}

			labels := make(map[string]string)
			labels["worker_id"] = batch.GetWorkerId()
			labels["test_run_id"] = testRunID
			for k, v := range m.GetLabels() {
				labels[k] = v
			}

			rows = append(rows, dao.Row{
				Metric:    m.GetName(),
				Labels:    labels,
				Timestamp: ts,
				Value:     m.GetValue(),
			})

			c.logger.Debug("Metric received and queued for storage",
				"worker_id", batch.GetWorkerId(),
				"test_run_id", testRunID,
				"labels", m.GetLabels(),
				"name", m.GetName(),
				"value", m.GetValue(),
			)
		}

		if err := storage.InsertRows(ctx, rows); err != nil {
			c.logger.Error("Failed to insert rows to storage", "test_run_id", testRunID, logging.ErrorAttr(err))
		}
	}
	if err := stream.Err(); err != nil {
		return nil, err
	}
	return connect.NewResponse(&swarunv1.SendMetricsResponse{Accepted: true}), nil
}

func (c *Controller) Heartbeat(
	ctx context.Context,
	req *connect.Request[swarunv1.HeartbeatRequest],
) (*connect.Response[swarunv1.HeartbeatResponse], error) {
	id := req.Msg.GetWorkerId()
	if w, ok := c.workers.Get(id); ok {
		// ハートビート時刻を更新
		w.LastHeartbeat = time.Now()
		c.workers.Store(id, w)
		return connect.NewResponse(&swarunv1.HeartbeatResponse{Acknowledged: true}), nil
	}

	c.logger.Warn("Heartbeat from unknown worker", "worker_id", id)
	return connect.NewResponse(&swarunv1.HeartbeatResponse{Acknowledged: false}), nil
}

func (c *Controller) ListWorkers(
	ctx context.Context,
	_ *connect.Request[swarunv1.ListWorkersRequest],
) (*connect.Response[swarunv1.ListWorkersResponse], error) {
	workersMap := c.workers.Load()
	workers := make([]*swarunv1.WorkerInfo, 0, len(workersMap))
	for _, w := range workersMap {
		workers = append(workers, &swarunv1.WorkerInfo{
			WorkerId:      w.ID,
			Hostname:      w.Hostname,
			Address:       w.Address,
			LastHeartbeat: timestamppb.New(w.LastHeartbeat),
		})
	}

	return connect.NewResponse(&swarunv1.ListWorkersResponse{
		Workers: workers,
	}), nil
}

func (c *Controller) GetMetrics(
	ctx context.Context,
	req *connect.Request[swarunv1.GetMetricsRequest],
) (*connect.Response[swarunv1.GetMetricsResponse], error) {
	testRunID := req.Msg.GetTestRunId()
	metricName := req.Msg.GetMetricName()
	labels := req.Msg.GetLabels()
	startTimeReq := req.Msg.GetStartTime()
	endTimeReq := req.Msg.GetEndTime()
	var startTime, endTime time.Time

	if startTimeReq != nil {
		startTime = startTimeReq.AsTime()
	}
	if endTimeReq != nil {
		endTime = endTimeReq.AsTime()
	}

	slog.Info("get metrics", "test_run_id", testRunID, "metric_name", metricName, "start_time", startTime, "end_time", endTime)

	if tr, ok := c.testRuns.Get(testRunID); ok {
		if startTimeReq == nil {
			startTime = tr.StartTime
		}
		if endTimeReq == nil {
			if !tr.EndTime.IsZero() {
				endTime = tr.EndTime
			} else {
				endTime = time.Now()
			}
		}
	}

	storage, err := c.getStorage(testRunID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to get storage: %w", err))
	}

	//labels := map[string]string{
	//	"test_run_id": testRunID,
	//}
	rows, err := storage.SelectRows(ctx, metricName, labels, startTime, endTime, req.Msg.GetAggregateFunc(), req.Msg.GetAggregateWindow().AsDuration())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to select rows: %w", err))
	}

	points := make([]*swarunv1.MetricData, 0, len(rows))
	for _, r := range rows {
		points = append(points, &swarunv1.MetricData{
			Timestamp: timestamppb.New(r.Timestamp),
			Value:     r.Value,
			Labels:    r.Labels,
		})
	}

	return connect.NewResponse(&swarunv1.GetMetricsResponse{
		Points: points,
	}), nil
}

type Exporter interface {
	Send(*swarunv1.ExportDataResponse) error
}

func (c *Controller) ExportData(
	ctx context.Context,
	_ *connect.Request[swarunv1.ExportDataRequest],
	stream *connect.ServerStream[swarunv1.ExportDataResponse],
) error {
	return c.exportData(ctx, stream)
}

func (c *Controller) exportData(
	ctx context.Context,
	stream Exporter,
) error {
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()
		zw := zip.NewWriter(pw)
		defer zw.Close()

		err := filepath.Walk(c.dataDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}

			relPath, err := filepath.Rel(c.dataDir, path)
			if err != nil {
				return err
			}

			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()

			zf, err := zw.Create(relPath)
			if err != nil {
				return err
			}

			_, err = io.Copy(zf, f)
			return err
		})

		if err != nil {
			c.logger.Error("Failed to zip data directory", "error", err)
			_ = pw.CloseWithError(err)
		}
	}()

	buf := make([]byte, 64*1024) // 64KB chunks
	for {
		n, err := pr.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&swarunv1.ExportDataResponse{Chunk: buf[:n]}); sendErr != nil {
				return sendErr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Controller) ImportData(
	ctx context.Context,
	stream *connect.ClientStream[swarunv1.ImportDataRequest],
) (*connect.Response[swarunv1.ImportDataResponse], error) {
	tempFile, err := os.CreateTemp("", "swarun-import-*.zip")
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to create temp file: %w", err))
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	for stream.Receive() {
		if _, err := tempFile.Write(stream.Msg().GetChunk()); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to write to temp file: %w", err))
		}
	}
	if err := stream.Err(); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("stream error: %w", err))
	}

	if _, err := tempFile.Seek(0, 0); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to seek temp file: %w", err))
	}

	stat, err := tempFile.Stat()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to stat temp file: %w", err))
	}

	zr, err := zip.NewReader(tempFile, stat.Size())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("failed to open zip: %w", err))
	}

	// 既存のデータをバックアップまたはクリアするか検討が必要だが、一旦上書きで実装
	for _, f := range zr.File {
		path := filepath.Join(c.dataDir, f.Name)
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(path, f.Mode()); err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to create directory: %w", err))
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to create directory: %w", err))
		}

		rc, err := f.Open()
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to open zip file member: %w", err))
		}

		dst, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			rc.Close()
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to create destination file: %w", err))
		}

		if _, err := io.Copy(dst, rc); err != nil {
			dst.Close()
			rc.Close()
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to extract file: %w", err))
		}
		dst.Close()
		rc.Close()
	}

	// データをインポートした後は、runs.json などを再読み込みする
	if err := c.loadTestRuns(); err != nil {
		c.logger.Warn("Failed to reload test runs after import", "error", err)
	}

	return connect.NewResponse(&swarunv1.ImportDataResponse{
		Success: true,
		Message: "Data imported successfully and test run history reloaded.",
	}), nil
}
