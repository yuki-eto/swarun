package cli

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"
	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/gen/proto/v1/swarunv1connect"
	"github.com/yuki-eto/swarun/internal/controller"
	"github.com/yuki-eto/swarun/internal/worker"
	"github.com/yuki-eto/swarun/pkg/config"
	"github.com/yuki-eto/swarun/pkg/logging"
	"github.com/yuki-eto/swarun/pkg/swarun"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	progressHeaderFormat = "\n%-10s %-10s %-10s %-10s %-10s %-10s %-15s\n"
	progressDataFormat   = "\r%-10s %-10d %-10d %-10.2f %-10d %-10d %-10.2f ms   "
)

func PrintTestProgressHeader() {
	fmt.Printf(progressHeaderFormat, "Elapsed", "VUs", "Workers", "RPS", "Success", "Failure", "Avg Latency")
	fmt.Println("-----------------------------------------------------------------------")
}

// PrintTestProgress はテスト実行中の進捗状況を表示します。
func PrintTestProgress(status *swarunv1.GetTestStatusResponse) {
	elapsed := time.Since(status.GetStartTime().AsTime()).Round(time.Second)
	fmt.Printf(progressDataFormat,
		elapsed,
		status.GetConcurrency(),
		status.GetWorkerCount(),
		status.GetRps(),
		status.GetTotalSuccess(),
		status.GetTotalFailure(),
		status.GetAvgLatencyMs(),
	)
}

// PrintTestSummary はテスト実行結果のサマリーを表示します。
func PrintTestSummary(status *swarunv1.GetTestStatusResponse) {
	fmt.Println("\n==================== Test Summary ====================")
	fmt.Printf("%-20s: %s\n", "Test Run ID", status.GetTestRunId())
	if status.GetDuration() != nil {
		fmt.Printf("%-20s: %s\n", "Duration", status.GetDuration().AsDuration().String())
	}
	fmt.Printf("%-20s: %d\n", "Total Success", status.GetTotalSuccess())
	fmt.Printf("%-20s: %d\n", "Total Failure", status.GetTotalFailure())
	fmt.Printf("%-20s: %.2f ms\n", "Avg Latency", status.GetAvgLatencyMs())
	fmt.Printf("%-20s: %.2f ms\n", "Max Latency", status.GetMaxLatencyMs())
	fmt.Printf("%-20s: %.2f ms\n", "Min Latency", status.GetMinLatencyMs())
	fmt.Printf("%-20s: %.2f ms\n", "90%% Latency", status.GetP90LatencyMs())
	fmt.Printf("%-20s: %.2f ms\n", "95%% Latency", status.GetP95LatencyMs())
	fmt.Printf("%-20s: %.2f req/s\n", "RPS", status.GetRps())
	fmt.Printf("%-20s: %d\n", "Concurrency", status.GetConcurrency())
	fmt.Printf("%-20s: %d\n", "Workers", status.GetWorkerCount())
	if !status.GetStartTime().AsTime().IsZero() {
		fmt.Printf("%-20s: %s\n", "Start Time", status.GetStartTime().AsTime().Format(time.RFC3339))
	}
	if !status.GetEndTime().AsTime().IsZero() {
		fmt.Printf("%-20s: %s\n", "End Time", status.GetEndTime().AsTime().Format(time.RFC3339))
	}

	// パスごとのメトリクスを表示
	if status.PathMetrics != nil && len(status.PathMetrics) > 0 {
		fmt.Println("\n-------------------- Path Metrics --------------------")
		fmt.Printf("%-30s %10s %10s %10s %10s\n", "Path", "Success", "Failure", "RPS", "Avg Latency")
		for path, m := range status.PathMetrics {
			fmt.Printf("%-30s %10d %10d %10.2f %10.2f ms\n",
				path,
				m.GetTotalSuccess(),
				m.GetTotalFailure(),
				m.GetRps(),
				m.GetAvgLatencyMs(),
			)
		}
	}
	fmt.Println("======================================================")
}

// ParseStages は文字列から RampingStage のリストをパースします。
func ParseStages(stagesStr string, logger *slog.Logger) []*swarunv1.RampingStage {
	if stagesStr == "" {
		return nil
	}
	var stages []*swarunv1.RampingStage
	parts := strings.Split(stagesStr, ",")
	for _, p := range parts {
		kv := strings.SplitN(p, ":", 2)
		if len(kv) == 2 {
			target, err := strconv.Atoi(kv[0])
			if err != nil {
				logger.Error("Failed to parse stage target", "stage", p, "error", err)
				continue
			}
			dur, err := time.ParseDuration(kv[1])
			if err != nil {
				logger.Error("Failed to parse stage duration", "stage", p, "error", err)
				continue
			}
			stages = append(stages, &swarunv1.RampingStage{
				Target:   int32(target),
				Duration: durationpb.New(dur),
			})
		}
	}
	return stages
}

func setupControllerMux(c *controller.Controller, logger *slog.Logger) (*http.ServeMux, string) {
	mux := http.NewServeMux()
	path, handler := swarunv1connect.NewControllerServiceHandler(c)

	// Add CORS support
	corsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Connect-Protocol-Version, Connect-Timeout-Ms, X-Grpc-Web, X-User-Agent")
		w.Header().Set("Access-Control-Max-Age", "3600")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		handler.ServeHTTP(w, r)
	})

	mux.Handle(path, corsHandler)

	// Static files for dashboard
	distFS, err := fs.Sub(staticFS, "web/dist")
	if err != nil {
		logger.Error("Failed to access embedded static files", logging.ErrorAttr(err))
	} else {
		fileServer := http.FileServer(http.FS(distFS))
		mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// gRPC パス以外でファイルが存在しない場合は index.html を返す (SPA routing)
			if !strings.HasPrefix(r.URL.Path, path) {
				f, err := distFS.Open(strings.TrimPrefix(r.URL.Path, "/"))
				if err != nil {
					// ファイルが見つからない場合は index.html を返す
					r.URL.Path = "/"
				} else {
					f.Close()
				}
			}
			fileServer.ServeHTTP(w, r)
		}))
	}

	return mux, path
}

func startServerWithGracefulShutdown(server *http.Server, logger *slog.Logger, componentName string) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := server.ListenAndServe(); err != nil {
			logger.Error(fmt.Sprintf("%s server failed", componentName), logging.ErrorAttr(err))
			os.Exit(1)
		}
	}()

	<-stop
	logger.Info(fmt.Sprintf("Shutting down %s...", strings.ToLower(componentName)))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Error(fmt.Sprintf("%s server shutdown failed", componentName), logging.ErrorAttr(err))
	}
}

func registerAndHeartbeat(ctx context.Context, client swarunv1connect.ControllerServiceClient, workerID, hostname, address string, heartbeatInterval time.Duration, logger *slog.Logger) {
	// Register with controller
	for {
		_, err := client.RegisterWorker(ctx, connect.NewRequest(&swarunv1.RegisterWorkerRequest{
			WorkerId: workerID,
			Hostname: hostname,
			Address:  address,
		}))
		if err == nil {
			logger.Info("Worker registered with controller", "worker_id", workerID, "address", address)
			break
		}
		logger.Warn("Failed to register with controller, retrying...", "worker_id", workerID, "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}

	// Heartbeat loop
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	failCount := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resp, err := client.Heartbeat(ctx, connect.NewRequest(&swarunv1.HeartbeatRequest{
				WorkerId: workerID,
			}))
			if err != nil || !resp.Msg.Acknowledged {
				failCount++
				if err != nil {
					logger.Warn("Heartbeat failed", "worker_id", workerID, "error", err, "fail_count", failCount)
				} else {
					logger.Warn("Heartbeat rejected by controller", "worker_id", workerID, "fail_count", failCount)
				}

				if failCount >= 3 {
					logger.Error("Heartbeat failed 3 consecutive times. Shutting down worker...", "worker_id", workerID)
					os.Exit(1)
				}
			} else {
				failCount = 0
			}
		}
	}
}

// RunController はコントローラーモードを実行します。
func RunController(cfg *config.Config, logger *slog.Logger) {
	runController(cfg, logger)
}

// RunWorker はワーカーモードを実行します。
func RunWorker(cfg *config.Config, sc swarun.Scenario, logger *slog.Logger) {
	runWorker(cfg, sc, logger)
}

// RunStandalone はスタンドアローンモードを実行します。
func RunStandalone(cfg *config.Config, sc swarun.Scenario, rampUp time.Duration, stages string, logger *slog.Logger) {
	runStandalone(cfg, sc, rampUp, stages, logger)
}

// staticFS は web/dist 配下のファイルを埋め込みます。
//
//go:embed all:web/dist
var staticFS embed.FS

func runController(cfg *config.Config, logger *slog.Logger) {
	c, err := controller.NewController(logger, cfg)
	if err != nil {
		logger.Error("Failed to initialize controller", logging.ErrorAttr(err))
		os.Exit(1)
	}
	defer c.Close()

	mux, _ := setupControllerMux(c, logger)

	logger.Info("Starting controller", "port", cfg.Port)
	server := &http.Server{
		Addr: fmt.Sprintf(":%d", cfg.Port),
		Handler: h2c.NewHandler(mux, &http2.Server{
			IdleTimeout:          1 * time.Minute,
			MaxReadFrameSize:     1024 * 1024,
			MaxConcurrentStreams: 1000,
		}),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	startServerWithGracefulShutdown(server, logger, "Controller")
}

func runWorker(cfg *config.Config, sc swarun.Scenario, logger *slog.Logger) {
	w := worker.NewWorker(cfg.WorkerID, cfg.ControllerAddr, sc, logger)
	mux := http.NewServeMux()
	path, handler := swarunv1connect.NewWorkerServiceHandler(w)
	mux.Handle(path, handler)

	// Register with controller
	go func() {
		client := swarunv1connect.NewControllerServiceClient(
			http.DefaultClient,
			cfg.ControllerAddr,
		)
		address := fmt.Sprintf("http://%s:%d", cfg.WorkerID, cfg.Port)
		hostname := cfg.WorkerID
		if h, err := os.Hostname(); err == nil {
			address = fmt.Sprintf("http://%s:%d", h, cfg.Port)
			hostname = h
		}

		registerAndHeartbeat(context.Background(), client, cfg.WorkerID, hostname, address, 10*time.Second, logger)
	}()

	logger.Info("Starting worker", "worker_id", cfg.WorkerID, "controller_addr", cfg.ControllerAddr, "port", cfg.Port)
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}

	startServerWithGracefulShutdown(server, logger, "Worker")
}

func runStandalone(cfg *config.Config, sc swarun.Scenario, rampUp time.Duration, stagesStr string, logger *slog.Logger) {
	// コントローラーの初期化
	c, err := controller.NewController(logger, cfg)
	if err != nil {
		logger.Error("Failed to initialize controller", logging.ErrorAttr(err))
		os.Exit(1)
	}
	defer c.Close()

	mux, _ := setupControllerMux(c, logger)

	// スタンドアローンなので、ControllerAddr は自分自身を指すようにする
	cfg.ControllerAddr = fmt.Sprintf("http://localhost:%d", cfg.Port)

	// ワーカーの初期化 (指定された数だけ作成)
	workerCount := cfg.WorkerCount
	if workerCount <= 0 {
		workerCount = 1
	}

	for i := 0; i < workerCount; i++ {
		workerID := cfg.WorkerID
		if workerID == "" {
			workerID = "standalone-worker"
		}
		if workerCount > 1 {
			workerID = fmt.Sprintf("%s-%d", workerID, i+1)
		}

		w := worker.NewWorker(workerID, cfg.ControllerAddr, sc, logger)
		// swarun パッケージがメトリクスを送信できるように環境変数を設定
		if i == 0 {
			os.Setenv("SWARUN_WORKER_ID", workerID)
			os.Setenv("SWARUN_CONTROLLER_ADDR", cfg.ControllerAddr)
		}

		// NOTE: 複数のワーカーを同一 ServeMux に登録する場合、ハンドラーのパスが重複する。
		// コントローラーが StartTest を呼ぶ際のベースURLを変えることで、パスレベルで出し分ける。
		// http.ServeMux はプレフィックス一致でルーティングするため、
		// "/worker-1/swarun.v1.WorkerService/StartTest" のようなリクエストを処理できるようにする。
		workerPathPrefix := fmt.Sprintf("/worker-%d", i+1)
		_, wHandler := swarunv1connect.NewWorkerServiceHandler(w)
		mux.Handle(workerPathPrefix+"/", http.StripPrefix(workerPathPrefix, wHandler))

		// ワーカーの登録処理
		go func(id string, prefix string) {
			client := swarunv1connect.NewControllerServiceClient(
				http.DefaultClient,
				cfg.ControllerAddr,
			)
			// アドレスにパスプレフィックスを含める
			address := cfg.ControllerAddr + prefix
			registerAndHeartbeat(context.Background(), client, id, "localhost", address, 30*time.Second, logger)
		}(workerID, workerPathPrefix)
	}

	// 自動実行モードが有効な場合、ワーカーが揃うのを待ってテストを開始
	if cfg.AutoStart {
		go func() {
			client := swarunv1connect.NewControllerServiceClient(
				http.DefaultClient,
				cfg.ControllerAddr,
			)
			// ワーカーが登録されるまで少し待機
			for {
				resp, err := client.ListWorkers(context.Background(), connect.NewRequest(&swarunv1.ListWorkersRequest{}))
				if err == nil && len(resp.Msg.GetWorkers()) >= workerCount {
					break
				}
				time.Sleep(500 * time.Millisecond)
			}

			logger.Info("All workers registered, starting test automatically", "worker_count", workerCount)
			stages := ParseStages(stagesStr, logger)

			req := &swarunv1.RunTestRequest{
				TestConfig: &swarunv1.StartTestRequest{
					Concurrency:    int32(cfg.Concurrency),
					Duration:       durationpb.New(cfg.Duration),
					TotalRequests:  cfg.TotalRequests,
					MaxDuration:    durationpb.New(cfg.Duration * 2),
					RampUpDuration: durationpb.New(rampUp),
					Stages:         stages,
				},
				// 全てのワーカーを対象にする
			}
			resp, err := client.RunTest(context.Background(), connect.NewRequest(req))
			if err != nil {
				logger.Error("Failed to auto-start test", logging.ErrorAttr(err))
				return
			}
			autoTestRunID := resp.Msg.GetTestRunId()

			// テストの終了を待機
			PrintTestProgressHeader()

			var finalStatus *swarunv1.GetTestStatusResponse
			for {
				time.Sleep(1 * time.Second)
				resp, err := client.GetTestStatus(context.Background(), connect.NewRequest(&swarunv1.GetTestStatusRequest{
					TestRunId: autoTestRunID,
				}))
				if err != nil {
					logger.Error("Failed to get test status during auto-start", logging.ErrorAttr(err))
					continue
				}

				PrintTestProgress(resp.Msg)

				if !resp.Msg.GetIsRunning() {
					fmt.Println() // 改行
					finalStatus = resp.Msg
					break
				}
			}

			logger.Info("Auto-start test completed")
			// 最終的なステータスを表示
			if finalStatus != nil {
				PrintTestSummary(finalStatus)
			}

			// しばらく待ってから終了（メトリクスの最終送信などのため）
			time.Sleep(2 * time.Second)
			os.Exit(0)
		}()
	}

	logger.Info("Starting standalone mode", "port", cfg.Port, "workers", workerCount, "auto_start", cfg.AutoStart)
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}

	startServerWithGracefulShutdown(server, logger, "Standalone")
}
