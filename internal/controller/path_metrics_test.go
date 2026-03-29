package controller

import (
	"context"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/pkg/config"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestController_PathMetrics_Percentiles(t *testing.T) {
	dataDir := "testdata/path_metrics"
	defer os.RemoveAll("testdata")

	cfg := config.DefaultConfig()
	cfg.DataDir = dataDir
	cfg.MetricsBackend = "duckdb"
	c, _ := NewController(nil, cfg)
	defer c.Close()

	testRunID := "test-path-metrics"
	tr := &TestRun{
		ID:          testRunID,
		StartTime:   time.Now().Add(-10 * time.Second),
		IsRunning:   true,
		PathMetrics: NewPathMetricsMap(),
	}
	c.testRuns.Store(testRunID, tr)

	ctx := context.Background()

	// メトリクスを送信 (pathA: 10, 20, 30, pathB: 100, 200, 300)
	latencies := map[string][]float64{
		"/api/a": {10, 20, 30},
		"/api/b": {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000},
	}

	for path, lats := range latencies {
		for _, lat := range lats {
			batch := &swarunv1.MetricBatch{
				WorkerId:  "worker-1",
				TestRunId: testRunID,
				Timestamp: timestamppb.Now(),
				Metrics: []*swarunv1.MetricEntry{
					{
						Name:   "success",
						Value:  1,
						Labels: map[string]string{"path": path},
					},
					{
						Name:   "latency_ms",
						Value:  lat,
						Labels: map[string]string{"path": path},
					},
				},
			}
			stream := &mockMetricStreamForFetch{msg: batch}
			c.sendMetrics(ctx, stream)
		}
	}

	// 1. 実行中のステータス確認
	req := connect.NewRequest(&swarunv1.GetTestStatusRequest{TestRunId: testRunID})
	resp, err := c.GetTestStatus(ctx, req)
	if err != nil {
		t.Fatalf("failed to get status: %v", err)
	}

	pmA := resp.Msg.PathMetrics["/api/a"]
	if pmA == nil {
		t.Fatal("path metrics for /api/a not found")
	}
	if pmA.P90LatencyMs != 30 {
		t.Errorf("expected /api/a P90 to be 30, got %f", pmA.P90LatencyMs)
	}

	pmB := resp.Msg.PathMetrics["/api/b"]
	if pmB == nil {
		t.Fatal("path metrics for /api/b not found")
	}
	// /api/b has 10 points. P90 index = ceil(0.9 * 10) - 1 = 8. Index 8 is 900.
	if pmB.P90LatencyMs != 900 {
		t.Errorf("expected /api/b P90 to be 900, got %f", pmB.P90LatencyMs)
	}

	// 2. テストを停止して、DuckDB からの再集計を確認
	tr.IsRunning = false
	tr.EndTime = time.Now()

	// インメモリの Latencies をクリアして、強制的にストレージから計算させる（またはキャッシュ動作を確認）
	// ただし、PathMetrics.Metrics["path"].Latencies もクリアする必要がある
	tr.PathMetrics.mu.Lock()
	for _, stats := range tr.PathMetrics.Metrics {
		stats.Latencies = nil
	}
	tr.PathMetrics.mu.Unlock()

	resp, err = c.GetTestStatus(ctx, req)
	if err != nil {
		t.Fatalf("failed to get status after stop: %v", err)
	}

	pmA = resp.Msg.PathMetrics["/api/a"]
	if pmA.P90LatencyMs != 30 {
		t.Errorf("expected /api/a P90 (from storage) to be 30, got %f", pmA.P90LatencyMs)
	}
	if pmA.P95LatencyMs != 30 {
		t.Errorf("expected /api/a P95 (from storage) to be 30, got %f", pmA.P95LatencyMs)
	}

	pmB = resp.Msg.PathMetrics["/api/b"]
	if pmB.P90LatencyMs != 900 {
		t.Errorf("expected /api/b P90 (from storage) to be 900, got %f", pmB.P90LatencyMs)
	}
	if pmB.P95LatencyMs != 1000 {
		t.Errorf("expected /api/b P95 (from storage) to be 1000, got %f", pmB.P95LatencyMs)
	}
}
