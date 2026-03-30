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
	// The path in staticFS is web/dist because the embed directive is web/dist
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

	// Graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Controller server failed", logging.ErrorAttr(err))
			os.Exit(1)
		}
	}()

	<-stop
	logger.Info("Shutting down controller...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("Server shutdown failed", logging.ErrorAttr(err))
	}
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
		if h, err := os.Hostname(); err == nil {
			address = fmt.Sprintf("http://%s:%d", h, cfg.Port)
		}

		for {
			_, err := client.RegisterWorker(context.Background(), connect.NewRequest(&swarunv1.RegisterWorkerRequest{
				WorkerId: cfg.WorkerID,
				Hostname: cfg.WorkerID,
				Address:  address,
			}))
			if err == nil {
				logger.Info("Worker registered with controller", "worker_id", cfg.WorkerID, "controller_addr", cfg.ControllerAddr)
				break
			}
			logger.Warn("Failed to register with controller, retrying in 5s", "error", err)
			time.Sleep(5 * time.Second)
		}

		// Heartbeat loop
		ticker := time.NewTicker(10 * time.Second)
		slog.Debug("Starting heartbeat loop")
		failCount := 0
		for range ticker.C {
			resp, err := client.Heartbeat(context.Background(), connect.NewRequest(&swarunv1.HeartbeatRequest{
				WorkerId: cfg.WorkerID,
			}))
			if err != nil || !resp.Msg.Acknowledged {
				failCount++
				if err != nil {
					logger.Warn("Heartbeat failed", "error", err, "fail_count", failCount)
				} else {
					logger.Warn("Heartbeat rejected by controller", "fail_count", failCount)
				}

				if failCount >= 3 {
					logger.Error("Heartbeat failed 3 consecutive times. Shutting down worker...")
					os.Exit(1)
				}
			} else {
				failCount = 0
			}
		}
	}()

	logger.Info("Starting worker", "worker_id", cfg.WorkerID, "controller_addr", cfg.ControllerAddr, "port", cfg.Port)
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}
	if err := server.ListenAndServe(); err != nil {
		logger.Error("Worker server failed", logging.ErrorAttr(err))
		os.Exit(1)
	}
}

func runStandalone(cfg *config.Config, sc swarun.Scenario, rampUp time.Duration, stagesStr string, logger *slog.Logger) {
	// コントローラーの初期化
	c, err := controller.NewController(logger, cfg)
	if err != nil {
		logger.Error("Failed to initialize controller", logging.ErrorAttr(err))
		os.Exit(1)
	}
	defer c.Close()

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
			for {
				_, err := client.RegisterWorker(context.Background(), connect.NewRequest(&swarunv1.RegisterWorkerRequest{
					WorkerId: id,
					Hostname: "localhost",
					Address:  address,
				}))
				if err == nil {
					logger.Info("Registered standalone worker", "id", id, "address", address)
					break
				}
				logger.Warn("Failed to register standalone worker, retrying...", "id", id, logging.ErrorAttr(err))
				time.Sleep(1 * time.Second)
			}

			// Heartbeat
			ticker := time.NewTicker(30 * time.Second)
			for range ticker.C {
				_, err := client.Heartbeat(context.Background(), connect.NewRequest(&swarunv1.HeartbeatRequest{
					WorkerId: id,
				}))
				if err != nil {
					logger.Warn("Failed to send heartbeat", "id", id, logging.ErrorAttr(err))
				}
			}
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
			var stages []*swarunv1.RampingStage
			if stagesStr != "" {
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
			}

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
			fmt.Printf("\n%-10s %-10s %-10s %-10s %-10s\n", "Elapsed", "RPS", "Success", "Failure", "Avg Latency")
			fmt.Println("---------------------------------------------------------------")

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

				msg := resp.Msg
				elapsed := time.Since(msg.GetStartTime().AsTime()).Round(time.Second)
				fmt.Printf("\r%-10s %-10.2f %-10d %-10d %-10.2f ms",
					elapsed,
					msg.GetRps(),
					msg.GetTotalSuccess(),
					msg.GetTotalFailure(),
					msg.GetAvgLatencyMs(),
				)

				if !resp.Msg.GetIsRunning() {
					fmt.Println() // 改行
					finalStatus = resp.Msg
					break
				}
			}

			logger.Info("Auto-start test completed")
			// 最終的なステータスを表示
			if finalStatus != nil {
				fmt.Println("\n==================== Test Summary ====================")
				fmt.Printf("%-20s: %s\n", "Test Run ID", finalStatus.GetTestRunId())
				fmt.Printf("%-20s: %s\n", "Duration", finalStatus.GetDuration().AsDuration().String())
				fmt.Printf("%-20s: %d\n", "Total Success", finalStatus.GetTotalSuccess())
				fmt.Printf("%-20s: %d\n", "Total Failure", finalStatus.GetTotalFailure())
				fmt.Printf("%-20s: %.2f ms\n", "Avg Latency", finalStatus.GetAvgLatencyMs())
				fmt.Printf("%-20s: %.2f ms\n", "Max Latency", finalStatus.GetMaxLatencyMs())
				fmt.Printf("%-20s: %.2f ms\n", "Min Latency", finalStatus.GetMinLatencyMs())
				fmt.Printf("%-20s: %.2f ms\n", "90%% Latency", finalStatus.GetP90LatencyMs())
				fmt.Printf("%-20s: %.2f ms\n", "95%% Latency", finalStatus.GetP95LatencyMs())
				fmt.Printf("%-20s: %.2f req/s\n", "RPS", finalStatus.GetRps())
				fmt.Printf("%-20s: %d\n", "Workers", finalStatus.GetWorkerCount())
				if !finalStatus.GetStartTime().AsTime().IsZero() {
					fmt.Printf("%-20s: %s\n", "Start Time", finalStatus.GetStartTime().AsTime().Format(time.RFC3339))
				}
				if !finalStatus.GetEndTime().AsTime().IsZero() {
					fmt.Printf("%-20s: %s\n", "End Time", finalStatus.GetEndTime().AsTime().Format(time.RFC3339))
				}

				// パスごとのメトリクスを表示
				if finalStatus.PathMetrics != nil && len(finalStatus.PathMetrics) > 0 {
					fmt.Println("\n-------------------- Path Metrics --------------------")
					fmt.Printf("%-30s %10s %10s %10s %10s\n", "Path", "Success", "Failure", "RPS", "Avg Latency")
					for path, m := range finalStatus.PathMetrics {
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
	if err := server.ListenAndServe(); err != nil {
		logger.Error("Standalone server failed", logging.ErrorAttr(err))
		os.Exit(1)
	}
}
