package controller

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/pkg/config"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestController_GetTestStatus_StorageFetch(t *testing.T) {
	dataDir := "testdata/storage_fetch"
	defer os.RemoveAll("testdata")

	cfg := config.DefaultConfig()
	cfg.DataDir = dataDir
	c, _ := NewController(nil, cfg)
	defer c.Close()

	testRunID := "test-run-fetch"
	c.testRuns.Store(testRunID, &TestRun{
		ID:           testRunID,
		StartTime:    time.Now().Add(-time.Minute),
		IsRunning:    true,
		PathMetrics:  NewPathMetricsMap(),
		LatenciesMu:  sync.RWMutex{},
	})

	ctx := context.Background()

	// ワーカーを登録
	c.workers.Store("worker-1", &Worker{ID: "worker-1", Address: "http://localhost:8081"})

	now := time.Now()
	for _, val := range []float64{100, 200, 300} {
		stream := &mockMetricStreamForFetch{
			msg: &swarunv1.MetricBatch{
				WorkerId:  "worker-1",
				TestRunId: testRunID,
				Timestamp: timestamppb.New(now),
				Metrics: []*swarunv1.MetricEntry{
					{
						Name:  "latency_ms",
						Value: val,
						Labels: map[string]string{
							"process": "binary_execution",
						},
					},
				},
			},
		}
		c.sendMetrics(ctx, stream)
		now = now.Add(time.Millisecond)
	}

	// ステータスを取得して、パーセンタイルが計算されているか確認
	req := connect.NewRequest(&swarunv1.GetTestStatusRequest{TestRunId: testRunID})
	resp, err := c.GetTestStatus(ctx, req)
	if err != nil {
		t.Fatalf("failed to get status: %v", err)
	}

	if resp.Msg.P90LatencyMs == 0 {
		t.Errorf("expected P90 latency to be non-zero, got 0.")
	} else if resp.Msg.P90LatencyMs != 300 {
		t.Errorf("expected P90 latency 300, got %f", resp.Msg.P90LatencyMs)
	}

	// 別のワーカーを追加して、合計のパーセンタイルが正しく計算されるか確認
	c.workers.Store("worker-2", &Worker{ID: "worker-2", Address: "http://localhost:8082"})
	stream2 := &mockMetricStreamForFetch{
		msg: &swarunv1.MetricBatch{
			WorkerId:  "worker-2",
			TestRunId: testRunID,
			Timestamp: timestamppb.Now(),
			Metrics: []*swarunv1.MetricEntry{
				{
					Name:  "latency_ms",
					Value: 500,
					Labels: map[string]string{
						"process": "binary_execution",
					},
				},
			},
		},
	}
	c.sendMetrics(ctx, stream2)

	resp, _ = c.GetTestStatus(ctx, req)
	// データは 100, 200, 300, 500. P90 は 500.
	if resp.Msg.P90LatencyMs != 500 {
		t.Errorf("expected P90 latency 500 after adding worker-2, got %f", resp.Msg.P90LatencyMs)
	}
}

type mockMetricStreamForFetch struct {
	msg      *swarunv1.MetricBatch
	err      error
	received bool
}

func (m *mockMetricStreamForFetch) Receive() bool {
	if m.received {
		return false
	}
	m.received = true
	return true
}

func (m *mockMetricStreamForFetch) Msg() *swarunv1.MetricBatch {
	return m.msg
}

func (m *mockMetricStreamForFetch) Err() error {
	return m.err
}
