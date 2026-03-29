package worker

import (
	"context"
	"net/http"

	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/gen/proto/v1/swarunv1connect"
	"github.com/yuki-eto/swarun/internal/runner"
	"github.com/yuki-eto/swarun/pkg/logging"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (w *Worker) reportMetrics(testRunID string, results chan runner.Result) {
	client := swarunv1connect.NewControllerServiceClient(
		http.DefaultClient,
		w.controllerAddr,
	)
	stream := client.SendMetrics(context.Background())
	defer stream.CloseAndReceive()

	for res := range results {
		metrics := []*swarunv1.MetricEntry{}
		if res.Latency == 0 {
			// 特別な終了通知
			metrics = append(metrics, &swarunv1.MetricEntry{
				Name:  "test_finished",
				Value: 1,
				Labels: map[string]string{
					"path": "scenario_iteration",
				},
			})
		} else {
			// 案Bにより、シナリオ単位の success/failure/latency_ms は送信しない
			continue
		}

		batch := &swarunv1.MetricBatch{
			WorkerId:  w.ID,
			TestRunId: testRunID,
			Timestamp: timestamppb.Now(),
			Metrics:   metrics,
		}
		if err := stream.Send(batch); err != nil {
			w.logger.Error("Failed to send metrics", logging.ErrorAttr(err))
		}
	}
}
