package worker

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"

	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/gen/proto/v1/swarunv1connect"
	"github.com/yuki-eto/swarun/internal/runner"
	"github.com/yuki-eto/swarun/pkg/logging"
	"github.com/yuki-eto/swarun/pkg/swarun"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (w *Worker) reportMetrics(testRunID string, results chan runner.Result) {
	// Configure HTTP/2 transport with Keepalive settings
	transport := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
			return net.Dial(network, addr)
		},
		ReadIdleTimeout:  15 * time.Second,
		PingTimeout:      15 * time.Second,
		WriteByteTimeout: 15 * time.Second,
	}

	httpClient := &http.Client{
		Transport: transport,
	}

	client := swarunv1connect.NewControllerServiceClient(
		httpClient,
		w.controllerAddr,
	)
	stream := client.SendMetrics(context.Background())
	defer func() {
		if _, err := stream.CloseAndReceive(); err != nil {
			w.logger.Error("Failed to close metrics stream", logging.ErrorAttr(err))
		}
	}()

	for res := range results {
		metrics := []*swarunv1.MetricEntry{}
		if res.Latency == 0 {
			// 全てのメトリクスが送信されたことを保証するために Flush を呼ぶ
			if err := swarun.Flush(context.Background()); err != nil {
				w.logger.Error("Failed to flush metrics before finishing", logging.ErrorAttr(err))
			}

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
