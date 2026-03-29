package swarun

import (
	"context"
	"io"
	"net/http"
	"net/http/httptrace"
	"os"
	"sync"
	"time"

	"connectrpc.com/connect"
	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/gen/proto/v1/swarunv1connect"
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
)

func initEnv() {
	once.Do(func() {
		workerID = os.Getenv("SWARUN_WORKER_ID")
		testRunID = os.Getenv("SWARUN_TEST_RUN_ID")
		controllerAddr = os.Getenv("SWARUN_CONTROLLER_ADDR")

		if controllerAddr != "" {
			client = swarunv1connect.NewControllerServiceClient(
				http.DefaultClient,
				controllerAddr,
			)
		}
	})
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
func SetTestRunID(id string) {
	testRunIDMu.Lock()
	defer testRunIDMu.Unlock()
	testRunID = id
}

// GetTestRunID returns the current test run ID.
func GetTestRunID() string {
	testRunIDMu.RLock()
	defer testRunIDMu.RUnlock()
	initEnv()
	return testRunID
}

func ReportMetrics(label string, metrics map[string]float64) {
	ctx := context.Background()
	s, err := getStream(ctx)
	trID := GetTestRunID()
	if err != nil || s == nil || workerID == "" || trID == "" {
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

	_ = s.Send(&swarunv1.MetricBatch{
		WorkerId:  workerID,
		TestRunId: trID,
		Timestamp: timestamppb.Now(),
		Metrics:   metricEntries,
	})
}

func ReportLatencies(label string, metrics map[string]time.Duration) {
	ctx := context.Background()
	s, err := getStream(ctx)
	trID := GetTestRunID()
	if err != nil || s == nil || workerID == "" || trID == "" {
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

	_ = s.Send(&swarunv1.MetricBatch{
		WorkerId:  workerID,
		TestRunId: trID,
		Timestamp: timestamppb.Now(),
		Metrics:   metricEntries,
	})
}

func ReportCustom(name string, value float64, labels map[string]string) {
	ctx := context.Background()
	s, err := getStream(ctx)
	trID := GetTestRunID()
	if err != nil || s == nil || workerID == "" || trID == "" {
		return
	}

	if labels == nil {
		labels = make(map[string]string)
	}
	if _, ok := labels["path"]; !ok {
		labels["path"] = "unknown"
	}

	_ = s.Send(&swarunv1.MetricBatch{
		WorkerId:  workerID,
		TestRunId: trID,
		Timestamp: timestamppb.Now(),
		Metrics: []*swarunv1.MetricEntry{
			{
				Name:   name,
				Value:  value,
				Labels: labels,
			},
		},
	})
}

// Flush はストリームを終了します。
func Flush(ctx context.Context) error {
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

	resp, err := http.DefaultClient.Do(req)
	totalLatency := time.Since(start)

	metrics := map[string]float64{
		"latency_ms": float64(totalLatency.Milliseconds()),
	}

	if err != nil {
		metrics["failure"] = 1
		ReportMetrics(label, metrics)
		return resp, err
	}

	// HTTP ステータスコードが 400 以上なら失敗として記録
	if resp.StatusCode >= 400 {
		metrics["failure"] = 1
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
				ReportCustom("response_size_bytes", float64(size), map[string]string{"path": label})
			},
		}
	}

	ReportMetrics(label, metrics)

	return resp, nil
}
