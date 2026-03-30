package runner

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/pkg/logging"
	"github.com/yuki-eto/swarun/pkg/swarun"
)

type Runner struct {
	workerID       string
	controllerAddr string
	scenario       swarun.Scenario
	req            *swarunv1.StartTestRequest
	results        chan Result
	cancel         context.CancelFunc
	totalRequests  atomic.Int64
	logger         *slog.Logger
}

func NewRunner(workerID, controllerAddr string, sc swarun.Scenario, req *swarunv1.StartTestRequest, results chan Result, logger *slog.Logger) *Runner {
	if logger == nil {
		logger = slog.Default()
	}
	return &Runner{
		workerID:       workerID,
		controllerAddr: controllerAddr,
		scenario:       sc,
		req:            req,
		results:        results,
		logger:         logger,
	}
}

func (r *Runner) Run(ctx context.Context) {
	ctx, r.cancel = context.WithCancel(ctx)

	// Set duration-based timeout if specified
	if r.req.GetDuration().AsDuration() > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.req.GetDuration().AsDuration())
		defer cancel()
	}

	// Set hard max duration timeout
	if r.req.GetMaxDuration().AsDuration() > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.req.GetMaxDuration().AsDuration())
		defer cancel()
	}

	stages := r.req.GetStages()
	if len(stages) > 0 {
		r.runStages(ctx, stages)
	} else {
		r.runLinearRampUp(ctx)
	}

	r.logger.Info("Runner finished")
	// 最後のメトリクスを確実に送信するために Flush を呼ぶ
	if err := swarun.Flush(ctx); err != nil {
		r.logger.Error("Failed to flush metrics in runner", logging.ErrorAttr(err))
	}
	r.results <- Result{Success: true, Latency: 0} // 特殊なメトリクスとして test_finished を送るためのダミー
}

func (r *Runner) runLinearRampUp(ctx context.Context) {
	var wg sync.WaitGroup
	concurrency := int(r.req.GetConcurrency())
	rampUp := r.req.GetRampUpDuration().AsDuration()

	r.logger.Info("Runner starting (linear ramp-up)",
		"concurrency", concurrency,
		"ramp_up", rampUp,
	)

	for i := range concurrency {
		if rampUp > 0 && i > 0 {
			select {
			case <-ctx.Done():
				break
			case <-time.After(rampUp / time.Duration(concurrency)):
			}
		}

		select {
		case <-ctx.Done():
			break
		default:
			wg.Add(1)
			go func() {
				defer wg.Done()
				r.worker(ctx)
			}()
		}
	}
	wg.Wait()
}

func (r *Runner) runStages(ctx context.Context, stages []*swarunv1.RampingStage) {
	r.logger.Info("Runner starting (stages)", "stages_count", len(stages))

	var wg sync.WaitGroup
	currentConcurrency := 0
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	for _, stage := range stages {
		target := int(stage.GetTarget())
		duration := stage.GetDuration().AsDuration()

		r.logger.Info("Starting stage", "target", target, "duration", duration, "current", currentConcurrency)

		if target > currentConcurrency {
			// ワーカーを増やす
			diff := target - currentConcurrency
			for range diff {
				if duration > 0 {
					// 期間内に徐々に増やす
					select {
					case <-ctx.Done():
						return
					case <-time.After(duration / time.Duration(diff)):
					}
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					r.worker(workerCtx)
				}()
			}
		} else if target < currentConcurrency {
			// ワーカーを減らす場合は現時点では非対応（コンテキストで全停止してから再起動か、動的制御が必要）
			// 簡単のため、ターゲットが減る場合はログを出して、維持する。
			r.logger.Warn("Reducing concurrency is not supported in stages, maintaining current", "target", target, "current", currentConcurrency)
		} else {
			// 維持
			select {
			case <-ctx.Done():
				return
			case <-time.After(duration):
			}
		}
		currentConcurrency = max(currentConcurrency, target)
	}

	// 全ステージ終了後、テスト全体の Duration が残っていれば待機
	if r.req.GetDuration().AsDuration() > 0 {
		<-ctx.Done()
	}

	cancelWorkers()
	wg.Wait()
}

func (r *Runner) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
}

func (r *Runner) worker(ctx context.Context) {
	totalLimit := r.req.GetTotalRequests()

	// swarun パッケージがメトリクスを送信できるように環境変数を設定
	// RunStandalone ですでに設定されている可能性があるが、ここでも確実に行う
	if swarun.GetTestRunID() == "" {
		swarun.SetTestRunID(r.req.GetTestRunId())
	}

	for {
		if totalLimit > 0 {
			if r.totalRequests.Add(1) > totalLimit {
				return
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
			// シナリオ実行用の環境変数は不要（同じプロセス内で動くため）
			// 必要なら Config 経由で渡すか、pkg/swarun/swarun.go で初期化する
			start := time.Now()
			err := r.scenario.Run(ctx)
			latency := time.Since(start)

			// Check if the error is due to context cancellation/timeout
			success := err == nil
			if err != nil {
				select {
				case <-ctx.Done():
					// If context is done, we don't count this as a failure/result
					return
				default:
					r.logger.Error("Scenario execution failed", logging.ErrorAttr(err))
				}
			}

			select {
			case r.results <- Result{Success: success, Latency: latency}:
			case <-ctx.Done():
				return
			}
		}
	}
}
