package controller

import (
	"sync"
	"time"
)

type TestRun struct {
	ID                  string          `json:"id"`
	StartTime           time.Time       `json:"start_time"`
	EndTime             time.Time       `json:"end_time"`
	ConfiguredDuration  time.Duration   `json:"configured_duration"`
	Concurrency         int32           `json:"concurrency"`
	IsRunning           bool            `json:"is_running"`
	TotalSuccess        int64           `json:"total_success"`
	TotalFailure        int64           `json:"total_failure"`
	TotalIterations     int64           `json:"total_iterations"`
	TotalLatency        time.Duration   `json:"total_latency"`
	LatencyCount        int64           `json:"latency_count"`
	WorkerCount         int32           `json:"worker_count"`
	FinishedWorkerCount int32           `json:"finished_worker_count"`
	MaxLatencyMs        float64         `json:"max_latency_ms"`
	MinLatencyMs        float64         `json:"min_latency_ms"`
	FirstRequestTime    time.Time       `json:"first_request_time"`
	LastRequestTime     time.Time       `json:"last_request_time"`
	Latencies           []float64       `json:"-"`
	LatenciesMu         sync.RWMutex    `json:"-"`
	PathMetrics         *PathMetricsMap `json:"path_metrics"`
}

type PathMetricsMap struct {
	mu      sync.RWMutex
	Metrics map[string]*PathStats `json:"metrics"`
}

type PathStats struct {
	Success      int64         `json:"success"`
	Failure      int64         `json:"failure"`
	Latencies    []float64     `json:"-"`
	TotalLatency time.Duration `json:"total_latency"`
	MinLatencyMs float64       `json:"min_latency_ms"`
	MaxLatencyMs float64       `json:"max_latency_ms"`
	P90LatencyMs float64       `json:"p90_latency_ms"`
	P95LatencyMs float64       `json:"p95_latency_ms"`
}

func NewPathMetricsMap() *PathMetricsMap {
	return &PathMetricsMap{
		Metrics: make(map[string]*PathStats),
	}
}

func (m *PathMetricsMap) Add(path, metric string, val float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	stats, ok := m.Metrics[path]
	if !ok {
		stats = &PathStats{}
		m.Metrics[path] = stats
	}

	switch metric {
	case "success":
		stats.Success += int64(val)
	case "failure":
		stats.Failure += int64(val)
	case "latency_ms":
		stats.Latencies = append(stats.Latencies, val)
		stats.TotalLatency += time.Duration(val * float64(time.Millisecond))
		if stats.MaxLatencyMs < val {
			stats.MaxLatencyMs = val
		}
		if stats.MinLatencyMs == 0 || stats.MinLatencyMs > val {
			stats.MinLatencyMs = val
		}
	}
}
