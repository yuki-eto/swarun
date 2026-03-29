package runner

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

type mockScenario struct {
	count atomic.Int64
}

func (s *mockScenario) Run(ctx context.Context) error {
	s.count.Add(1)
	return nil
}

func TestRunner_Stages(t *testing.T) {
	results := make(chan Result, 1000)
	sc := &mockScenario{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	req := &swarunv1.StartTestRequest{
		TestRunId: "test-stages",
		Stages: []*swarunv1.RampingStage{
			{Target: 2, Duration: durationpb.New(2 * time.Second)}, // 2ワーカーに2秒かけて増やす
			{Target: 2, Duration: durationpb.New(2 * time.Second)}, // 2ワーカー維持
		},
	}

	r := NewRunner("worker-1", "http://localhost:8080", sc, req, results, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	go r.Run(ctx)

	// 進捗を監視して、ワーカー数が増えているか大まかに確認したいが、
	// チャンネルからメトリクスを吸い出す必要がある
	go func() {
		for range results {
			// メトリクスを消費し続ける
		}
	}()

	// 4秒程度で終わるはず
	time.Sleep(5 * time.Second)
	r.Stop()

	elapsed := time.Since(start)
	t.Logf("Elapsed: %v, Total Requests: %d", elapsed, sc.count.Load())

	if sc.count.Load() == 0 {
		t.Errorf("Expected some requests to be made, but got 0")
	}
}
