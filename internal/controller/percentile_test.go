package controller

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/internal/dao"
	"github.com/yuki-eto/swarun/pkg/config"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type mockMetricStreamForPercentile struct {
	batch *swarunv1.MetricBatch
	done  bool
}

func (m *mockMetricStreamForPercentile) Receive() bool {
	if m.done {
		return false
	}
	m.done = true
	return true
}
func (m *mockMetricStreamForPercentile) Msg() *swarunv1.MetricBatch { return m.batch }
func (m *mockMetricStreamForPercentile) Err() error                 { return nil }

func TestController_PercentileCalculation(t *testing.T) {
	dataDir := "data-percentile-test"
	os.RemoveAll(dataDir)
	defer os.RemoveAll(dataDir)

	cfg := config.DefaultConfig()
	cfg.DataDir = dataDir
	cfg.MetricsBackend = "duckdb"

	c, err := NewController(slog.New(slog.NewTextHandler(os.Stdout, nil)), cfg)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	defer c.Close()

	testRunID := "test-percentile"
	c.testRuns.Store(testRunID, &TestRun{
		ID:        testRunID,
		StartTime: time.Now().Add(-1 * time.Minute),
		IsRunning: true,
	})

	// 100個のデータを投入 (1ms, 2ms, ..., 100ms)
	// 90パーセンタイルは 90ms, 95パーセンタイルは 95ms になるはず
	var metrics []*swarunv1.MetricEntry
	for i := 1; i <= 100; i++ {
		metrics = append(metrics, &swarunv1.MetricEntry{
			Name:  "latency_ms",
			Value: float64(i),
		})
	}

	// 直接 DAO に挿入して確実性を高める
	storage, _ := c.getStorage(testRunID)
	rowsToInsert := make([]dao.Row, 100)
	for i := 1; i <= 100; i++ {
		rowsToInsert[i-1] = dao.Row{
			Metric:    "latency_ms",
			Value:     float64(i),
			Timestamp: time.Now(),
		}
	}
	storage.InsertRows(context.Background(), rowsToInsert)

	// データの書き込みを待つ (tstorage のフラッシュ)
	storage.Close()
	c.storages.Clear() // 再度 getStorage されるように

	// データの存在を確認
	newStorage, _ := c.getStorage(testRunID)
	rows, err := newStorage.SelectRows(context.Background(), "latency_ms", nil, time.Now().Add(-1*time.Hour), time.Now().Add(1*time.Hour), "", 0)
	if err != nil {
		t.Fatalf("failed to select rows for verification: %v", err)
	}
	fmt.Printf("Number of rows found: %d\n", len(rows))
	if len(rows) == 0 {
		t.Fatalf("no rows found in storage")
	}

	// 直接インメモリにも投入してインメモリ集計の動作を確認
	tr, _ := c.testRuns.Get(testRunID)
	tr.LatenciesMu.Lock()
	for i := 1; i <= 100; i++ {
		tr.Latencies = append(tr.Latencies, float64(i))
	}
	tr.LatenciesMu.Unlock()

	resp, err := c.GetTestStatus(context.Background(), connect.NewRequest(&swarunv1.GetTestStatusRequest{
		TestRunId: testRunID,
	}))
	if err != nil {
		t.Fatalf("failed to get test status: %v", err)
	}

	fmt.Printf("P90: %.2f, P95: %.2f\n", resp.Msg.GetP90LatencyMs(), resp.Msg.GetP95LatencyMs())

	// 現在の実装 latencies[int(float64(len(latencies))*0.9)] では
	// 100 * 0.9 = 90.0 -> index 90 -> 91ms (1-indexed なので)
	// 100 * 0.95 = 95.0 -> index 95 -> 96ms
	// になっている可能性がある。

	if resp.Msg.GetP90LatencyMs() != 90.0 {
		t.Errorf("expected P90 to be 90.0, got %.2f", resp.Msg.GetP90LatencyMs())
	}
	if resp.Msg.GetP95LatencyMs() != 95.0 {
		t.Errorf("expected P95 to be 95.0, got %.2f", resp.Msg.GetP95LatencyMs())
	}

	// ステータスを取得して、パス別メトリクスが正しく計算されているか確認
	// シナリオを模したメトリクスを投入
	c.sendMetrics(context.Background(), &mockMetricStreamForPercentile{
		batch: &swarunv1.MetricBatch{
			WorkerId:  "worker-1",
			TestRunId: testRunID,
			Timestamp: timestamppb.Now(),
			Metrics: []*swarunv1.MetricEntry{
				{Name: "success", Value: 10, Labels: map[string]string{"path": "/api/v1"}},
				{Name: "failure", Value: 2, Labels: map[string]string{"path": "/api/v1"}},
				{Name: "latency_ms", Value: 100, Labels: map[string]string{"path": "/api/v1"}},
				{Name: "latency_ms", Value: 200, Labels: map[string]string{"path": "/api/v1"}},
			},
		},
	})

	resp2, err := c.GetTestStatus(context.Background(), connect.NewRequest(&swarunv1.GetTestStatusRequest{
		TestRunId: testRunID,
	}))
	if err != nil {
		t.Fatalf("failed to get test status: %v", err)
	}

	pm, ok := resp2.Msg.PathMetrics["/api/v1"]
	if !ok {
		t.Fatalf("expected path metrics for /api/v1, but not found")
	}
	if pm.TotalSuccess != 10 {
		t.Errorf("expected success 10, got %d", pm.TotalSuccess)
	}
	if pm.TotalFailure != 2 {
		t.Errorf("expected failure 2, got %d", pm.TotalFailure)
	}
	if pm.AvgLatencyMs != 150 {
		t.Errorf("expected avg latency 150, got %f", pm.AvgLatencyMs)
	}
	if pm.P95LatencyMs != 200 {
		t.Errorf("expected P95 latency 200, got %f", pm.P95LatencyMs)
	}
}

func TestController_PercentileCalculationSmall(t *testing.T) {
	dataDir := "data-percentile-small-test"
	os.RemoveAll(dataDir)
	defer os.RemoveAll(dataDir)

	cfg := config.DefaultConfig()
	cfg.DataDir = dataDir
	cfg.MetricsBackend = "duckdb"

	c, err := NewController(slog.New(slog.NewTextHandler(os.Stdout, nil)), cfg)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	defer c.Close()

	testRunID := "test-percentile-small"
	c.testRuns.Store(testRunID, &TestRun{
		ID:        testRunID,
		StartTime: time.Now().Add(-1 * time.Minute),
		IsRunning: true,
	})

	// 10個のデータ投入 (10, 20, ..., 100)
	// 最近傍法では
	// P90: idx = ceil(0.9 * 10) - 1 = 9 - 1 = 8 -> 90ms
	// P95: idx = ceil(0.95 * 10) - 1 = 10 - 1 = 9 -> 100ms

	// 直接 DAO に挿入して確実性を高める
	storageSmall, _ := c.getStorage(testRunID)
	rowsSmall := make([]dao.Row, 10)
	for i := 1; i <= 10; i++ {
		rowsSmall[i-1] = dao.Row{
			Metric:    "latency_ms",
			Value:     float64(i * 10),
			Timestamp: time.Now(),
		}
	}
	storageSmall.InsertRows(context.Background(), rowsSmall)

	// データの書き込みを待つ (tstorage のフラッシュ)
	storageSmall.Close()
	c.storages.Clear()

	// 直接インメモリにも投入してインメモリ集計の動作を確認
	trSmall, _ := c.testRuns.Get(testRunID)
	trSmall.LatenciesMu.Lock()
	for i := 1; i <= 10; i++ {
		trSmall.Latencies = append(trSmall.Latencies, float64(i*10))
	}
	trSmall.LatenciesMu.Unlock()

	resp, err := c.GetTestStatus(context.Background(), connect.NewRequest(&swarunv1.GetTestStatusRequest{
		TestRunId: testRunID,
	}))
	if err != nil {
		t.Fatalf("failed to get test status: %v", err)
	}

	fmt.Printf("Small P90: %.2f, P95: %.2f\n", resp.Msg.GetP90LatencyMs(), resp.Msg.GetP95LatencyMs())
	if resp.Msg.GetP90LatencyMs() != 90.0 {
		t.Errorf("expected small P90 to be 90.0, got %.2f", resp.Msg.GetP90LatencyMs())
	}
	if resp.Msg.GetP95LatencyMs() != 100.0 {
		t.Errorf("expected small P95 to be 100.0, got %.2f", resp.Msg.GetP95LatencyMs())
	}
}
