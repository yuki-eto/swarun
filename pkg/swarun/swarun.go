package swarun

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/gen/proto/v1/swarunv1connect"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	once           sync.Once
	client         swarunv1connect.ControllerServiceClient
	workerID       string
	testRunID      string
	controllerAddr string

	// testRunIDMu protects testRunID for dynamic updates
	testRunIDMu sync.RWMutex

	// stream is the gRPC stream to send metrics.
	// It's initialized on first send.
	stream *connect.ClientStreamForClient[swarunv1.MetricBatch, swarunv1.SendMetricsResponse]
	mu     sync.Mutex

	// loadTestClient は負荷試験のリクエスト用のカスタム HTTP クライアントです。
	loadTestClient *http.Client

	// controllerClient はコントローラーとの通信用の HTTP クライアントです。
	controllerClient *http.Client

	metricQueue chan *swarunv1.MetricEntry
	wg          sync.WaitGroup
	done        chan struct{}
)

var (
	// batchMu protects pendingMetrics
	batchMu        sync.Mutex
	pendingMetrics []*swarunv1.MetricEntry
	lastFlush      time.Time
)

const (
	queueSize     = 100000
	batchSize     = 1000
	batchInterval = 100 * time.Millisecond
)

func initEnv() {
	once.Do(func() {
		workerID = os.Getenv("SWARUN_WORKER_ID")
		testRunID = os.Getenv("SWARUN_TEST_RUN_ID")
		controllerAddr = os.Getenv("SWARUN_CONTROLLER_ADDR")

		metricQueue = make(chan *swarunv1.MetricEntry, queueSize)
		done = make(chan struct{})

		// 負荷試験用の Transport 設定
		// MaxIdleConnsPerHost を増やして、同一ホストへの同時接続性能を向上させる
		t := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          10000,
			MaxIdleConnsPerHost:   2000, // Concurrency 設定より大きい値が望ましい
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     true,
		}

		loadTestClient = &http.Client{
			Transport: t,
			Timeout:   60 * time.Second,
		}

		if controllerAddr != "" {
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

			controllerClient = &http.Client{
				Transport: transport,
			}

			client = swarunv1connect.NewControllerServiceClient(
				controllerClient,
				controllerAddr,
			)

			go asyncSender()
		}
	})
}

func asyncSender() {
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	for {
		select {
		case entry, ok := <-metricQueue:
			if !ok {
				flushBatch()
				close(done)
				return
			}
			batchMu.Lock()
			pendingMetrics = append(pendingMetrics, entry)
			shouldFlush := len(pendingMetrics) >= batchSize
			batchMu.Unlock()

			if shouldFlush {
				flushBatch()
			}
		case <-ticker.C:
			flushBatch()
		}
	}
}

func flushBatch() {
	batchMu.Lock()
	if len(pendingMetrics) == 0 {
		batchMu.Unlock()
		return
	}
	entries := pendingMetrics
	pendingMetrics = nil
	batchMu.Unlock()

	trID := GetTestRunID()
	if trID == "" {
		return
	}

	batch := &swarunv1.MetricBatch{
		WorkerId:  workerID,
		TestRunId: trID,
		Timestamp: timestamppb.Now(),
		Metrics:   entries,
	}

	sendWithRetry(batch)
	for range len(entries) {
		wg.Done()
	}
}

func sendWithRetry(batch *swarunv1.MetricBatch) {
	backoff := 100 * time.Millisecond
	maxBackoff := 5 * time.Second

	for {
		ctx := context.Background()
		s, err := getStream(ctx)
		if err != nil || s == nil {
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		if err := s.Send(batch); err != nil {
			slog.Error("Failed to send metrics in asyncSender, retrying...", "error", err, "workerID", batch.WorkerId)
			mu.Lock()
			stream = nil
			mu.Unlock()

			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}
		return
	}
}

func getStream(ctx context.Context) (*connect.ClientStreamForClient[swarunv1.MetricBatch, swarunv1.SendMetricsResponse], error) {
	mu.Lock()
	defer mu.Unlock()
	if stream != nil {
		return stream, nil
	}
	initEnv()
	if client == nil {
		return nil, nil
	}
	stream = client.SendMetrics(ctx)
	return stream, nil
}

// ReportFailure はラベルを指定して失敗を記録し、即座に送信します。
func ReportFailure(label string) {
	ReportMetrics(label, map[string]float64{"failure": 1})
}

// SetTestRunID sets the current test run ID.
// If the test run ID is different from the current one, the existing stream is closed and reset.
func SetTestRunID(id string) {
	testRunIDMu.Lock()
	defer testRunIDMu.Unlock()
	if testRunID != id {
		testRunID = id
		// Reset stream for new test run
		mu.Lock()
		if stream != nil {
			// Closing current stream (non-blocking attempt)
			go func(s *connect.ClientStreamForClient[swarunv1.MetricBatch, swarunv1.SendMetricsResponse]) {
				s.CloseAndReceive()
			}(stream)
			stream = nil
		}
		mu.Unlock()
	}
}

// GetTestRunID returns the current test run ID.
func GetTestRunID() string {
	testRunIDMu.RLock()
	defer testRunIDMu.RUnlock()
	initEnv()
	return testRunID
}

func ReportMetrics(label string, metrics map[string]float64) {
	initEnv()
	trID := GetTestRunID()
	if workerID == "" || trID == "" {
		return
	}

	pathLabels := make(map[string]string)
	if label != "" {
		pathLabels["path"] = label
	} else {
		pathLabels["path"] = "unknown"
	}

	metricEntries := []*swarunv1.MetricEntry{}
	for name, val := range metrics {
		metricEntries = append(metricEntries, &swarunv1.MetricEntry{
			Name:   name,
			Value:  val,
			Labels: pathLabels,
		})
	}

	for _, entry := range metricEntries {
		wg.Add(1)
		select {
		case metricQueue <- entry:
		default:
			slog.Warn("Metric queue is full, dropping metric", "label", label)
			wg.Done()
		}
	}
}

func ReportLatencies(label string, metrics map[string]time.Duration) {
	initEnv()
	trID := GetTestRunID()
	if workerID == "" || trID == "" {
		return
	}

	pathLabels := make(map[string]string)
	if label != "" {
		pathLabels["path"] = label
	} else {
		pathLabels["path"] = "unknown"
	}

	metricEntries := []*swarunv1.MetricEntry{}
	for name, d := range metrics {
		metricEntries = append(metricEntries, &swarunv1.MetricEntry{
			Name:   name,
			Value:  float64(d.Milliseconds()),
			Labels: pathLabels,
		})
	}

	for _, entry := range metricEntries {
		wg.Add(1)
		select {
		case metricQueue <- entry:
		default:
			slog.Warn("Metric queue is full, dropping metric", "label", label)
			wg.Done()
		}
	}
}

func ReportCustom(name string, value float64, labels map[string]string) {
	initEnv()
	trID := GetTestRunID()
	if workerID == "" || trID == "" {
		return
	}

	if labels == nil {
		labels = make(map[string]string)
	}
	if _, ok := labels["path"]; !ok {
		labels["path"] = "unknown"
	}

	entry := &swarunv1.MetricEntry{
		Name:   name,
		Value:  value,
		Labels: labels,
	}

	wg.Add(1)
	select {
	case metricQueue <- entry:
	default:
		slog.Warn("Metric queue is full, dropping metric", "metric", name)
		wg.Done()
	}
}

// Flush はキュー内の全てのメトリクスが送信されるのを待機し、ストリームを終了します。
func Flush(ctx context.Context) error {
	initEnv()
	if metricQueue == nil {
		return nil
	}

	// Wait for all pending metrics in the queue to be sent
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if stream == nil {
		return nil
	}
	_, err := stream.CloseAndReceive()
	stream = nil
	return err
}

// Get は http.Get をラップし、メトリクスを自動的に記録します。
// リクエスト時間とレスポンス時間も個別に記録されます。
func Get(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		ReportFailure(url)
		return nil, err
	}
	return Do(req)
}

// Post は http.Post をラップし、メトリクスを自動的に記録します。
// リクエスト時間とレスポンス時間も個別に記録されます。
func Post(url, contentType string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		ReportFailure(url)
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	return Do(req)
}

type sizeTrackingReader struct {
	io.ReadCloser
	size    int64
	onClose func(int64)
}

func (r *sizeTrackingReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.size += int64(n)
	return n, err
}

func (r *sizeTrackingReader) Close() error {
	err := r.ReadCloser.Close()
	if r.onClose != nil {
		r.onClose(r.size)
	}
	return err
}

// Do は http.Client.Do をラップし、メトリクスを自動的に記録します。
// リクエスト時間（送信完了まで）とレスポンス時間（受信開始から完了まで）も個別に記録されます。
// また、レスポンスサイズも計測されます。
func Do(req *http.Request) (*http.Response, error) {
	// リクエスト ID の生成 (UUID v7)
	requestID, err := uuid.NewV7()
	var reqIDStr string
	if err != nil {
		slog.Warn("failed to generate uuid v7, falling back to v4", "error", err)
		reqIDStr = uuid.New().String()
	} else {
		reqIDStr = requestID.String()
	}

	label := ""
	if req.URL != nil {
		label = req.URL.String()
	}

	var (
		start           = time.Now()
		requestSent     time.Time
		responseStarted time.Time
	)

	trace := &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			requestSent = time.Now()
		},
		GotFirstResponseByte: func() {
			responseStarted = time.Now()
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	initEnv() // Ensure client is initialized
	resp, err := loadTestClient.Do(req)
	totalLatency := time.Since(start)

	metrics := map[string]float64{
		"latency_ms": float64(totalLatency.Milliseconds()),
	}

	labels := map[string]string{
		"path":       label,
		"method":     req.Method,
		"request_id": reqIDStr,
	}

	if err != nil {
		metrics["failure"] = 1
		for name, val := range metrics {
			ReportCustom(name, val, labels)
		}
		return resp, err
	}

	// HTTP ステータスコードが 400 以上なら失敗として記録
	if resp.StatusCode >= 400 {
		metrics["failure"] = 1

		// レスポンス内容をログに出力
		limitedReader := io.LimitReader(resp.Body, 1024)
		bodyBytes, err := io.ReadAll(limitedReader)
		if err == nil {
			slog.Error("Request failed",
				"status_code", resp.StatusCode,
				"url", label,
				"method", req.Method,
				"response_body", string(bodyBytes),
				"request_id", reqIDStr,
			)
		}
		// Body を差し替えておかないと後続の処理で読めなくなる
		resp.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	} else {
		// 成功を記録
		metrics["success"] = 1
	}

	metrics["status_code"] = float64(resp.StatusCode)
	if resp.ContentLength >= 0 {
		metrics["response_size_bytes"] = float64(resp.ContentLength)
	}

	if !requestSent.IsZero() {
		metrics["request_latency_ms"] = float64(requestSent.Sub(start).Milliseconds())
	}
	if !responseStarted.IsZero() {
		metrics["response_latency_ms"] = float64(time.Since(responseStarted).Milliseconds())
		metrics["ttfb_ms"] = float64(responseStarted.Sub(start).Milliseconds())
	}

	// ContentLength が負（不明）の場合のみ、Body をラップして実測する
	if resp.ContentLength < 0 {
		resp.Body = &sizeTrackingReader{
			ReadCloser: resp.Body,
			onClose: func(size int64) {
				labelsCopy := maps.Clone(labels)
				ReportCustom("response_size_bytes", float64(size), labelsCopy)
			},
		}
	}

	for name, val := range metrics {
		ReportCustom(name, val, labels)
	}

	return resp, nil
}
