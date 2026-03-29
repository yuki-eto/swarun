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

type mockMetricStream struct {
	msg      *swarunv1.MetricBatch
	err      error
	received bool
}

func (m *mockMetricStream) Receive() bool {
	if m.received {
		return false
	}
	m.received = true
	return true
}

func (m *mockMetricStream) Msg() *swarunv1.MetricBatch {
	return m.msg
}

func (m *mockMetricStream) Err() error {
	return m.err
}

func TestController_MetricsAndStatus(t *testing.T) {
	dataDir := "testdata/controller"
	defer os.RemoveAll("testdata")

	cfg := config.DefaultConfig()
	cfg.DataDir = dataDir
	c, _ := NewController(nil, cfg)
	defer c.Close()

	testRunID := "test-run-metrics"
	c.testRuns.Store(testRunID, &TestRun{
		ID:        testRunID,
		StartTime: time.Now(),
		IsRunning: true,
	})

	ctx := context.Background()

	// 100ms のレイテンシを報告
	stream1 := &mockMetricStream{
		msg: &swarunv1.MetricBatch{
			TestRunId: testRunID,
			Timestamp: timestamppb.Now(),
			Metrics: []*swarunv1.MetricEntry{
				{Name: "success", Value: 1},
				{Name: "latency_ms", Value: 100},
			},
		},
	}
	c.sendMetrics(ctx, stream1)

	// 200ms のレイテンシを報告
	stream2 := &mockMetricStream{
		msg: &swarunv1.MetricBatch{
			TestRunId: testRunID,
			Timestamp: timestamppb.Now(),
			Metrics: []*swarunv1.MetricEntry{
				{Name: "success", Value: 1},
				{Name: "latency_ms", Value: 200},
			},
		},
	}
	c.sendMetrics(ctx, stream2)

	// ステータスを確認
	req := connect.NewRequest(&swarunv1.GetTestStatusRequest{TestRunId: testRunID})
	resp, err := c.GetTestStatus(ctx, req)
	if err != nil {
		t.Fatalf("failed to get status: %v", err)
	}

	if resp.Msg.TotalSuccess != 2 {
		t.Errorf("expected 2 success, got %d", resp.Msg.TotalSuccess)
	}
	if resp.Msg.AvgLatencyMs != 150.0 {
		t.Errorf("expected 150.0 ms average latency, got %f", resp.Msg.AvgLatencyMs)
	}
	if resp.Msg.MaxLatencyMs != 200.0 {
		t.Errorf("expected 200.0 ms max latency, got %f", resp.Msg.MaxLatencyMs)
	}
	if resp.Msg.MinLatencyMs != 100.0 {
		t.Errorf("expected 100.0 ms min latency, got %f", resp.Msg.MinLatencyMs)
	}
}
