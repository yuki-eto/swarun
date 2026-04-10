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
	"google.golang.org/protobuf/types/known/structpb"
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
		workers:      atomicmap.New[string, *Worker](),
		storages:     atomicmap.New[string, dao.MetricsDAO](),
		testRuns:     atomicmap.New[string, *TestRun](),
		orchestrator: orchestrator.NewOrchestrator(logger, cfg),
		logger:       logger,
		cfg:          cfg,
		dataDir:      cfg.DataDir,
	}
	if cfg.S3 != nil {
		c.defaultS3Bucket = cfg.S3.Bucket
		c.defaultS3Region = cfg.S3.Region
		c.defaultS3Prefix = cfg.S3.Prefix
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
				Method:       stats.Method,
				Success:      int64(stats.Success),
				Failure:      int64(stats.Failure),
				TotalLatency: time.Duration(stats.AvgLatencyMs * (stats.Success + stats.Failure) * float64(time.Millisecond)),
				MinLatencyMs: stats.MinLatencyMs,
				MaxLatencyMs: stats.MaxLatencyMs,
			}
			tr.PathMetrics.mu.Unlock()

			if path == "scenario_iteration" {
				tr.TotalIterations = int64(stats.Success)
			} else {
				tr.TotalSuccess += int64(stats.Success)
				tr.TotalFailure += int64(stats.Failure)
				tr.TotalLatency += time.Duration(stats.AvgLatencyMs * (stats.Success + stats.Failure) * float64(time.Millisecond))
				tr.LatencyCount += int64(stats.Success + stats.Failure)
				if tr.MaxLatencyMs < stats.MaxLatencyMs {
					tr.MaxLatencyMs = stats.MaxLatencyMs
				}
				if tr.MinLatencyMs == 0 || tr.MinLatencyMs > stats.MinLatencyMs {
					tr.MinLatencyMs = stats.MinLatencyMs
				}
			}
		}
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
	if err := c.orchestrator.TeardownAll(context.Background()); err != nil {
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
		id, err := uuid.NewV7()
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to generate test run id: %w", err))
		}
		testRunID = id.String()
		testConfig.TestRunId = testRunID
	}

	allWorkers := c.workers.Load()
	var workersToStart []*Worker

	for _, w := range allWorkers {
		workersToStart = append(workersToStart, w)
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
		AutoExportS3:       testConfig.GetAutoExportS3(),
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

	// テストが終了している、またはパーセンタイル計算のためにストレージから再集計
	// 実行中でも DuckDB なら十分高速に計算可能
	storage, err := c.getStorage(testRunID)
	if err == nil {
		overallStats, pathStats, err := storage.SelectStats(ctx, nil, tr.StartTime, time.Now())
		if err == nil {
			// 全体の統計
			if lat, ok := overallStats["p90_latency"]; ok {
				p90 = lat
			}
			if lat, ok := overallStats["p95_latency"]; ok {
				p95 = lat
			}
			if lat, ok := overallStats["avg_latency"]; ok {
				avgLatency = lat
			}

			// パスごとの統計
			for path, stats := range pathStats {
				pm := &swarunv1.PathMetrics{
					Method:       stats.Method,
					TotalSuccess: int64(stats.Success),
					TotalFailure: int64(stats.Failure),
					AvgLatencyMs: stats.AvgLatencyMs,
					MaxLatencyMs: stats.MaxLatencyMs,
					MinLatencyMs: stats.MinLatencyMs,
					P90LatencyMs: stats.P90LatencyMs,
					P95LatencyMs: stats.P95LatencyMs,
				}
				if duration > 0 {
					pm.Rps = float64(pm.TotalSuccess+pm.TotalFailure) / duration.Seconds()
				}
				pathMetrics[path] = pm

				// インメモリのキャッシュも可能な限り更新
				if tr.PathMetrics != nil {
					tr.PathMetrics.mu.Lock()
					if s, ok := tr.PathMetrics.Metrics[path]; ok {
						s.Success = pm.TotalSuccess
						s.Failure = pm.TotalFailure
						s.MinLatencyMs = pm.MinLatencyMs
						s.MaxLatencyMs = pm.MaxLatencyMs
						s.P90LatencyMs = pm.P90LatencyMs
						s.P95LatencyMs = pm.P95LatencyMs
					} else {
						tr.PathMetrics.Metrics[path] = &PathStats{
							Method:       pm.Method,
							Success:      pm.TotalSuccess,
							Failure:      pm.TotalFailure,
							MinLatencyMs: pm.MinLatencyMs,
							MaxLatencyMs: pm.MaxLatencyMs,
							P90LatencyMs: pm.P90LatencyMs,
							P95LatencyMs: pm.P95LatencyMs,
						}
					}
					tr.PathMetrics.mu.Unlock()
				}
			}
		}
	}

	if avgLatency == 0 && tr.LatencyCount > 0 {
		avgLatency = float64(tr.TotalLatency.Milliseconds()) / float64(tr.LatencyCount)
	}

	// Calculate duration for RPS
	actualDurationSec := float64(1)
	if !tr.LastRequestTime.IsZero() && !tr.FirstRequestTime.IsZero() {
		if tr.LastRequestTime.After(tr.FirstRequestTime) {
			actualDurationSec = tr.LastRequestTime.Sub(tr.FirstRequestTime).Seconds()
		}
	}

	if actualDurationSec < 1 && duration > 0 {
		actualDurationSec = duration.Seconds()
	}

	if actualDurationSec < 1 {
		actualDurationSec = 1
	}

	// 成功/失敗数が 0 の場合は RPS 0
	if tr.TotalSuccess+tr.TotalFailure == 0 {
		rps = 0
	} else {
		rps = float64(tr.TotalSuccess+tr.TotalFailure) / actualDurationSec
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
				Method:       stats.Method,
				TotalSuccess: stats.Success,
				TotalFailure: stats.Failure,
			}
			if stats.TotalLatency > 0 && (stats.Success+stats.Failure) > 0 {
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
		TestRunId:        tr.ID,
		IsRunning:        tr.IsRunning,
		TotalSuccess:     tr.TotalSuccess,
		TotalFailure:     tr.TotalFailure,
		AvgLatencyMs:     avgLatency,
		WorkerCount:      tr.WorkerCount,
		StartTime:        timestamppb.New(tr.StartTime),
		MaxLatencyMs:     tr.MaxLatencyMs,
		MinLatencyMs:     tr.MinLatencyMs,
		P90LatencyMs:     p90,
		P95LatencyMs:     p95,
		Rps:              rps,
		EndTime:          timestamppb.New(tr.EndTime),
		PathMetrics:      pathMetrics,
		Duration:         durationpb.New(tr.ConfiguredDuration),
		Concurrency:      tr.Concurrency,
		TotalIterations:  tr.TotalIterations,
		FirstRequestTime: timestamppb.New(tr.FirstRequestTime),
		LastRequestTime:  timestamppb.New(tr.LastRequestTime),
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
	if err := c.orchestrator.TeardownAll(ctx); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// 全ワーカーの情報を削除
	c.workers.Clear()

	return connect.NewResponse(&swarunv1.TeardownWorkersResponse{
		Success: true,
		Message: "Successfully teared down all workers",
	}), nil
}

func (c *Controller) TeardownWorker(
	ctx context.Context,
	req *connect.Request[swarunv1.TeardownWorkerRequest],
) (*connect.Response[swarunv1.TeardownWorkerResponse], error) {
	workerID := req.Msg.GetWorkerId()
	if workerID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("worker_id is required"))
	}

	// オーケストレーターで停止処理
	if err := c.orchestrator.Teardown(ctx, workerID); err != nil {
		c.logger.Warn("Failed to teardown worker in orchestrator", "worker_id", workerID, "error", err)
		// オーケストレーターで見つからなくても、メモリ上のリストから消すことは試みる
	}

	// メモリ上のワーカーリストから削除
	// c.workers は atomicmap.Map[string, *Worker] で、Delete メソッドがあるか確認が必要
	// atomic_map.go を見ると Delete がないかもしれない
	c.removeWorker(workerID)

	return connect.NewResponse(&swarunv1.TeardownWorkerResponse{
		Success: true,
		Message: fmt.Sprintf("Successfully teared down worker %s", workerID),
	}), nil
}

func (c *Controller) removeWorker(id string) {
	m := c.workers.Load()
	if _, ok := m[id]; !ok {
		return
	}
	newMap := maps.Clone(m)
	delete(newMap, id)
	c.workers.Swap(newMap)
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
	testRuns := c.testRuns.Load()
	ids := slices.Collect(maps.Keys(testRuns))
	slices.Sort(ids)
	slices.Reverse(ids) // 新しい順に表示

	summaries := make([]*swarunv1.TestRunSummary, 0, len(ids))
	for _, id := range ids {
		tr, ok := testRuns[id]
		if !ok {
			continue
		}

		var rps float64
		var avgLatency float64
		duration := time.Since(tr.StartTime)
		if !tr.IsRunning {
			duration = tr.EndTime.Sub(tr.StartTime)
		}

		if duration > 0 {
			rps = float64(tr.TotalSuccess) / duration.Seconds()
		}
		if tr.LatencyCount > 0 {
			avgLatency = float64(tr.TotalLatency.Milliseconds()) / float64(tr.LatencyCount)
		}

		summaries = append(summaries, &swarunv1.TestRunSummary{
			TestRunId:    id,
			StartTime:    timestamppb.New(tr.StartTime),
			IsRunning:    tr.IsRunning,
			Concurrency:  tr.Concurrency,
			WorkerCount:  tr.WorkerCount,
			Rps:          rps,
			AvgLatencyMs: avgLatency,
		})
	}

	return connect.NewResponse(&swarunv1.ListTestRunsResponse{
		TestRuns: summaries,
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
				// 最初と最後のリクエスト時刻を記録
				if tr.FirstRequestTime.IsZero() || ts.Before(tr.FirstRequestTime) {
					tr.FirstRequestTime = ts
				}
				if ts.After(tr.LastRequestTime) {
					tr.LastRequestTime = ts
				}

				// パスごとのメトリクスを集計 (インメモリ)
				path := m.GetLabels()["path"]
				if path == "" {
					path = "unknown"
				}
				method := m.GetLabels()["method"]

				if tr.PathMetrics == nil {
					tr.PathMetrics = NewPathMetricsMap()
				}
				tr.PathMetrics.Add(path, method, m.GetName(), m.GetValue())

				if path == "scenario_iteration" {
					if m.GetName() == "success" {
						atomic.AddInt64(&tr.TotalIterations, int64(m.GetValue()))
					}
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

						if tr.MaxLatencyMs < val {
							tr.MaxLatencyMs = val
						}
						if tr.MinLatencyMs == 0 || tr.MinLatencyMs > val {
							tr.MinLatencyMs = val
						}
					}
				}

				if m.GetName() == "test_finished" {
					finishedCount := atomic.AddInt32(&tr.FinishedWorkerCount, 1)
					c.logger.Info("Received test_finished", "test_run_id", testRunID, "worker_id", batch.GetWorkerId(), "finished_count", finishedCount, "total_workers", tr.WorkerCount)
					if finishedCount >= tr.WorkerCount {
						// 全ワーカーの終了通知を受信
						// メトリクス送信の最終バッチがストレージに書き込まれるのを確実にするため、
						// 少し猶予を持たせてから Running 状態を false にする
						go func(targetID string) {
							time.Sleep(2 * time.Second)
							if tr, ok := c.testRuns.Get(targetID); ok {
								tr.IsRunning = false
								tr.EndTime = time.Now()
								c.logger.Info("Test run finished", "test_run_id", targetID, "success", tr.TotalSuccess, "failure", tr.TotalFailure, "end_time", tr.EndTime)

								// インメモリモードの場合はデータをエクスポートする
								if c.cfg.DuckDBInMemoryMode {
									if storage, err := c.getStorage(targetID); err == nil {
										destPath := filepath.Join(c.dataDir, targetID)
										if err := storage.Export(destPath); err != nil {
											c.logger.Error("Failed to export in-memory database", "test_run_id", targetID, "error", err)
										} else {
											c.logger.Info("Successfully exported in-memory database", "test_run_id", targetID, "dest", destPath)
										}
									}
								}

								if err := c.saveTestRuns(); err != nil {
									c.logger.Error("Failed to save test runs on finish", "error", err)
								}

								// 自動エクスポートが有効な場合
								if tr.AutoExportS3 {
									go func(id string) {
										_, err := c.ExportToS3(context.Background(), connect.NewRequest(&swarunv1.ExportToS3Request{
											TestRunId: id,
										}))
										if err != nil {
											c.logger.Error("Failed to auto-export to S3", "test_run_id", id, "error", err)
										} else {
											c.logger.Info("Successfully auto-exported to S3", "test_run_id", id)
										}

										// S3 エクスポート後にストレージをクリーンアップ
										// DuckDB インメモリモードで保持されているメモリを解放する
										if s, ok := c.storages.Get(id); ok {
											if err := s.Close(); err != nil {
												c.logger.Error("Failed to close storage after S3 export", "test_run_id", id, "error", err)
											}
											c.storages.Delete(id)
											c.logger.Info("Closed and deleted storage after S3 export", "test_run_id", id)
										}
									}(targetID)
								} else {
									// S3 エクスポートがない場合はここでストレージをクリーンアップ
									// DuckDB インメモリモードで保持されているメモリを解放する
									if s, ok := c.storages.Get(targetID); ok {
										if err := s.Close(); err != nil {
											c.logger.Error("Failed to close storage after test finish", "test_run_id", targetID, "error", err)
										}
										c.storages.Delete(targetID)
										c.logger.Info("Closed and deleted storage after test finish", "test_run_id", targetID)
									}
								}
							}
						}(testRunID)
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
				WorkerID:  batch.GetWorkerId(),
				Path:      m.GetLabels()["path"],
				Method:    m.GetLabels()["method"],
				RequestID: m.GetLabels()["request_id"],
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

func (c *Controller) QueryMetrics(ctx context.Context, req *connect.Request[swarunv1.QueryMetricsRequest]) (*connect.Response[swarunv1.QueryMetricsResponse], error) {
	testRunID := req.Msg.GetTestRunId()
	query := req.Msg.GetQuery()

	// 1. 新しいコネクション（DAO）を個別に作成する
	// c.getStorage を使うと internal キャッシュに保持されてしまうため、直接作成する
	var storage dao.MetricsDAO
	var err error

	switch c.cfg.MetricsBackend {
	case "influxdb":
		storage, err = dao.NewInfluxDBDAO(
			ctx,
			c.cfg.InfluxDBURL,
			c.cfg.InfluxDBToken,
			c.cfg.InfluxDBOrg,
			c.cfg.InfluxDBBucket,
			testRunID,
		)
	default:
		storage, err = dao.NewDuckDBDAO(c.dataDir, testRunID, false)
	}

	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to open storage for %s: %w", testRunID, err))
	}
	// 2. 実行後に必ず閉じる
	defer storage.Close()

	// 3. クエリ実行
	rawRows, cols, err := storage.QueryRaw(ctx, query)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("query failed: %w", err))
	}

	// 1000件に制限
	const maxRows = 1000
	if len(rawRows) > maxRows {
		rawRows = rawRows[:maxRows]
	}

	var respRows []*swarunv1.QueryResultRow
	for _, row := range rawRows {
		s, err := structpb.NewStruct(row)
		if err != nil {
			// structpb.NewStruct は、値がサポートされていない場合（例: func, chan）にエラーを返します。
			// map[string]any の中身をログに出力するなどしてデバッグを容易にします。
			c.logger.Error("Failed to convert row to structpb.Struct", "error", err, "row", row)
			continue
		}
		respRows = append(respRows, &swarunv1.QueryResultRow{Columns: s})
	}

	return connect.NewResponse(&swarunv1.QueryMetricsResponse{Rows: respRows, ColumnNames: cols}), nil
}

func (c *Controller) IsS3Enabled() bool {
	return c.defaultS3Bucket != ""
}
