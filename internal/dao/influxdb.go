package dao

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

type influxDBDAO struct {
	client    influxdb2.Client
	writeAPI  api.WriteAPIBlocking
	queryAPI  api.QueryAPI
	org       string
	bucket    string
	testRunID string
}

// NewInfluxDBDAO は InfluxDB をバックエンドとする MetricsDAO を作成します。
// bucket 名に bucketPrefix を使用し、内部で testRunID によるフィルタリングを行います。
func NewInfluxDBDAO(ctx context.Context, url, token, org, bucket, testRunID string) (MetricsDAO, error) {
	client := influxdb2.NewClient(url, token)

	bucketName := bucket
	if bucketName == "" {
		bucketName = "swarun"
	}

	// バケットが存在するか確認し、なければ作成する
	bucketsAPI := client.BucketsAPI()
	b, err := bucketsAPI.FindBucketByName(ctx, bucketName)
	if err != nil || b == nil {
		orgAPI := client.OrganizationsAPI()
		o, err := orgAPI.FindOrganizationByName(ctx, org)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("failed to find organization %s: %w", org, err)
		}

		_, err = bucketsAPI.CreateBucketWithName(ctx, o, bucketName)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("failed to create bucket %s: %w", bucketName, err)
		}
	}

	writeAPI := client.WriteAPIBlocking(org, bucketName)
	queryAPI := client.QueryAPI(org)

	return &influxDBDAO{
		client:    client,
		writeAPI:  writeAPI,
		queryAPI:  queryAPI,
		org:       org,
		bucket:    bucketName,
		testRunID: testRunID,
	}, nil
}

func (d *influxDBDAO) InsertRows(ctx context.Context, rows []Row) error {
	points := make([]*write.Point, 0, len(rows))
	for _, r := range rows {
		labels := make(map[string]string)
		for k, v := range r.Labels {
			labels[k] = v
		}
		if d.testRunID != "" {
			labels["test_run_id"] = d.testRunID
		}
		if r.WorkerID != "" {
			labels["worker_id"] = r.WorkerID
		}
		if r.Path != "" {
			labels["path"] = r.Path
		}
		if r.RequestID != "" {
			labels["request_id"] = r.RequestID
		}

		p := influxdb2.NewPoint(
			r.Metric,
			labels,
			map[string]any{"value": r.Value},
			r.Timestamp,
		)
		points = append(points, p)
	}

	if err := d.writeAPI.WritePoint(ctx, points...); err != nil {
		return fmt.Errorf("failed to write points to InfluxDB: %w", err)
	}
	return nil
}

func (d *influxDBDAO) SelectRows(ctx context.Context, metric string, labels map[string]string, start, end time.Time, aggregateFunc string, aggregateWindow time.Duration) ([]Row, error) {
	slog.Info("select rows", "metric", metric, "labels", labels, "start", start, "end", end, "aggregate_func", aggregateFunc, "aggregate_window", aggregateWindow)
	// Flux クエリの組み立て
	// range の stop は排他的なため、ミリ秒ほど足しておく
	query := fmt.Sprintf(`from(bucket: "%s")
		|> range(start: time(v: "%s"), stop: time(v: "%s"))
		|> filter(fn: (r) => r["_measurement"] == "%s")`,
		d.bucket,
		start.Format(time.RFC3339Nano),
		end.Add(time.Millisecond).Format(time.RFC3339Nano),
		metric,
	)

	if d.testRunID != "" {
		query += fmt.Sprintf(` |> filter(fn: (r) => r["test_run_id"] == "%s")`, d.testRunID)
	}

	for k, v := range labels {
		if k == "test_run_id" {
			continue
		}
		query += fmt.Sprintf(` |> filter(fn: (r) => r["%s"] == "%s")`, k, v)
	}

	// 集計処理の追加
	if aggregateFunc != "" {
		window := aggregateWindow
		if window == 0 {
			window = time.Second // デフォルト 1s
		}
		query += fmt.Sprintf(` |> aggregateWindow(every: %s, fn: %s, createEmpty: false)`, window.String(), aggregateFunc)
	}

	slog.Info("execute query", "query", query)
	result, err := d.queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query InfluxDB: %w", err)
	}
	defer result.Close()

	var rows []Row
	for result.Next() {
		r := result.Record()
		val, ok := r.Value().(float64)
		if !ok {
			// InfluxDB は型が混ざることがあるため、int64 なども考慮
			if v, ok := r.Value().(int64); ok {
				val = float64(v)
			}
		}

		row := Row{
			Metric:    metric,
			Timestamp: r.Time(),
			Value:     val,
			WorkerID:  fmt.Sprintf("%v", r.ValueByKey("worker_id")),
			Path:      fmt.Sprintf("%v", r.ValueByKey("path")),
			RequestID: fmt.Sprintf("%v", r.ValueByKey("request_id")),
			Labels:    make(map[string]string),
		}

		// その他のラベルを詰め直す
		for k, v := range r.Values() {
			if k != "worker_id" && k != "path" && k != "request_id" && !strings.HasPrefix(k, "_") && k != "test_run_id" {
				row.Labels[k] = fmt.Sprintf("%v", v)
			}
		}
		rows = append(rows, row)
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("error during InfluxDB query result iteration: %w", result.Err())
	}

	return rows, nil
}

func (d *influxDBDAO) SelectStats(ctx context.Context, labels map[string]string, start, end time.Time) (map[string]float64, map[string]map[string]float64, error) {
	// InfluxDB では未実装（とりあえず空で返す）
	return make(map[string]float64), make(map[string]map[string]float64), nil
}

func (d *influxDBDAO) QueryRaw(ctx context.Context, query string) ([]map[string]any, []string, error) {
	result, err := d.queryAPI.Query(ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query InfluxDB: %w", err)
	}
	defer result.Close()

	var rows []map[string]any
	var columnNames []string
	columnSet := make(map[string]struct{})

	for result.Next() {
		r := result.Record()
		m := make(map[string]any)
		for k, v := range r.Values() {
			m[k] = v
			if _, ok := columnSet[k]; !ok {
				columnSet[k] = struct{}{}
				columnNames = append(columnNames, k)
			}
		}
		rows = append(rows, m)
	}

	if result.Err() != nil {
		return nil, nil, fmt.Errorf("error during InfluxDB query result iteration: %w", result.Err())
	}

	return rows, columnNames, nil
}

func (d *influxDBDAO) Export(destPath string) error {
	// InfluxDB では未実装
	return nil
}

func (d *influxDBDAO) Close() error {
	d.client.Close()
	return nil
}
