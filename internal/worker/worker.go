package worker

import (
	"context"
	"log/slog"
	"sync"

	"connectrpc.com/connect"
	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/internal/runner"
	"github.com/yuki-eto/swarun/pkg/swarun"
)

type Worker struct {
	ID             string
	controllerAddr string
	scenario       swarun.Scenario
	mu             sync.Mutex
	currentRunner  *runner.Runner
	logger         *slog.Logger
}

func NewWorker(id string, controllerAddr string, sc swarun.Scenario, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		ID:             id,
		controllerAddr: controllerAddr,
		scenario:       sc,
		logger:         logger,
	}
}

func (w *Worker) StartTest(
	ctx context.Context,
	req *connect.Request[swarunv1.StartTestRequest],
) (*connect.Response[swarunv1.StartTestResponse], error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentRunner != nil {
		w.currentRunner.Stop()
	}

	results := make(chan runner.Result, 100)
	testRunID := req.Msg.GetTestRunId()
	swarun.SetTestRunID(testRunID)
	w.currentRunner = runner.NewRunner(w.ID, w.controllerAddr, w.scenario, req.Msg, results, w.logger)

	go w.currentRunner.Run(context.Background())
	go w.reportMetrics(testRunID, results)

	return connect.NewResponse(&swarunv1.StartTestResponse{
		Started: true,
		Message: "Test started",
	}), nil
}

func (w *Worker) StopTest(
	ctx context.Context,
	req *connect.Request[swarunv1.StopTestRequest],
) (*connect.Response[swarunv1.StopTestResponse], error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentRunner != nil {
		w.currentRunner.Stop()
		w.currentRunner = nil
	}

	return connect.NewResponse(&swarunv1.StopTestResponse{
		Stopped: true,
	}), nil
}
