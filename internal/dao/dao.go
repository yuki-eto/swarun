package dao

import (
	"context"
	"time"
)

// Row は単一のメトリクス行を表します。
type Row struct {
	Metric    string
	Timestamp time.Time
	Value     float64
	WorkerID  string            // 追加
	Path      string            // 追加
	Method    string            // 追加
	RequestID string            // 追加
	Labels    map[string]string // その他のカスタムラベル
}

// PathStats はパスごとの統計情報を表します。
type PathStats struct {
	Method       string
	Success      float64
	Failure      float64
	AvgLatencyMs float64
	MaxLatencyMs float64
	MinLatencyMs float64
	P90LatencyMs float64
	P95LatencyMs float64
}

// MetricsDAO はメトリクスの保存と読み取りを担当するインターフェースです。
type MetricsDAO interface {
	// InsertRows は複数のメトリクス行を保存します。
	InsertRows(ctx context.Context, rows []Row) error
	// SelectRows は指定されたメトリクスとラベルに一致し、[start, end] の範囲内にあるメトリクス行を取得します。
	// aggregateFunc や aggregateWindow が指定されている場合は集計を行います。
	SelectRows(ctx context.Context, metric string, labels map[string]string, start, end time.Time, aggregateFunc string, aggregateWindow time.Duration) ([]Row, error)
	// SelectStats は指定されたメトリクスの統計（成功数、失敗数、平均レイテンシなど）を取得します。
	// labels でフィルタリング可能です。
	SelectStats(ctx context.Context, labels map[string]string, start, end time.Time) (map[string]float64, map[string]PathStats, error)
	// QueryRaw はバックエンド固有のクエリ（DuckDBならSQL、InfluxDBならFlux）を直接実行します。
	QueryRaw(ctx context.Context, query string) ([]map[string]any, []string, error)
	// Export はデータを指定されたパスにエクスポートします。
	Export(destPath string) error
	// Close はリソースを解放します。
	Close() error
}
