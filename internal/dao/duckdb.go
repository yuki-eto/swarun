package dao

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

type duckDBDAO struct {
	db        *sql.DB
	testRunID string
}

// NewDuckDBDAO は DuckDB をバックエンドとする MetricsDAO を作成します。
func NewDuckDBDAO(dataDir, testRunID string) (MetricsDAO, error) {
	path := filepath.Join(dataDir, testRunID)
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory %s: %w", path, err)
	}

	dbPath := filepath.Join(path, "metrics.duckdb")
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb at %s: %w", dbPath, err)
	}

	// テーブルの作成
	// DuckDB では JSON 型を扱うために json エクステンションが必要な場合があるが、
	// go-duckdb では標準で含まれていることが多い。
	// ラベルを文字列型で保存し、集計時に JSON として扱うか、あるいは最初から JSON 型にする。
	_, err = db.Exec(`
		INSTALL json;
		LOAD json;
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load json extension: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS metrics (
			timestamp TIMESTAMP,
			metric TEXT,
			value DOUBLE,
			labels JSON
		)
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return &duckDBDAO{
		db:        db,
		testRunID: testRunID,
	}, nil
}

func (d *duckDBDAO) InsertRows(ctx context.Context, rows []Row) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, "INSERT INTO metrics (timestamp, metric, value, labels) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range rows {
		labelsJSON, err := json.Marshal(r.Labels)
		if err != nil {
			return err
		}
		if _, err := stmt.ExecContext(ctx, r.Timestamp, r.Metric, r.Value, string(labelsJSON)); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (d *duckDBDAO) SelectRows(ctx context.Context, metric string, labels map[string]string, start, end time.Time, aggregateFunc string, aggregateWindow time.Duration) ([]Row, error) {
	var query string
	var args []any

	if aggregateFunc != "" && aggregateWindow > 0 {
		// SQL による集計
		sqlFunc := strings.ToUpper(aggregateFunc)
		// DuckDB の time_bucket を使用してウィンドウ集計を行う
		// window_str := fmt.Sprintf("%d seconds", int(aggregateWindow.Seconds()))
		// ただし time_bucket は秒単位などの文字列指定が必要

		query = fmt.Sprintf(`
			SELECT 
				time_bucket(CAST(? AS INTERVAL), timestamp) as bucket,
				%s(value) as val
			FROM metrics
			WHERE metric = ? AND timestamp >= ? AND timestamp <= ?
		`, sqlFunc)

		intervalStr := fmt.Sprintf("%d milliseconds", aggregateWindow.Milliseconds())
		args = append(args, intervalStr, metric, start, end)

		// ラベルフィルタリング
		if labels == nil {
			labels = make(map[string]string)
		}
		for k, v := range labels {
			query += " AND json_extract_path_text(labels, ?) = ?"
			args = append(args, "/"+k, v)
		}

		query += " GROUP BY bucket ORDER BY bucket"
	} else {
		// 生データの取得
		query = "SELECT timestamp, value, labels FROM metrics WHERE metric = ? AND timestamp >= ? AND timestamp <= ?"
		args = append(args, metric, start, end)

		if labels == nil {
			labels = make(map[string]string)
		}
		for k, v := range labels {
			query += " AND json_extract_path_text(labels, ?) = ?"
			args = append(args, "/"+k, v)
		}
		query += " ORDER BY timestamp"
	}

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Row
	for rows.Next() {
		var r Row
		r.Metric = metric
		if aggregateFunc != "" && aggregateWindow > 0 {
			if err := rows.Scan(&r.Timestamp, &r.Value); err != nil {
				return nil, err
			}
		} else {
			var labelsVal any
			if err := rows.Scan(&r.Timestamp, &r.Value, &labelsVal); err != nil {
				return nil, err
			}
			if labelsVal == nil {
				r.Labels = make(map[string]string)
			} else {
				switch v := labelsVal.(type) {
				case string:
					if err := json.Unmarshal([]byte(v), &r.Labels); err != nil {
						return nil, err
					}
				case []byte:
					if err := json.Unmarshal(v, &r.Labels); err != nil {
						return nil, err
					}
				case map[string]any:
					r.Labels = make(map[string]string)
					for k, val := range v {
						r.Labels[k] = fmt.Sprintf("%v", val)
					}
				default:
					return nil, fmt.Errorf("unexpected type for labels: %T", labelsVal)
				}
			}
		}
		result = append(result, r)
	}

	return result, nil
}

func (d *duckDBDAO) SelectStats(ctx context.Context, labels map[string]string, start, end time.Time) (map[string]float64, map[string]map[string]float64, error) {
	// 全体の統計を取得
	overallQuery := `
		SELECT 
			metric,
			SUM(value) as total,
			AVG(value) as avg,
			MAX(value) as max,
			MIN(value) as min,
			COUNT(value) as count
		FROM metrics
		WHERE timestamp >= ? AND timestamp <= ?
	`
	var overallArgs []any
	overallArgs = append(overallArgs, start, end)

	for k, v := range labels {
		overallQuery += " AND json_extract_path_text(labels, ?) = ?"
		overallArgs = append(overallArgs, "/"+k, v)
	}
	overallQuery += " GROUP BY metric"

	overallRows, err := d.db.QueryContext(ctx, overallQuery, overallArgs...)
	if err != nil {
		return nil, nil, err
	}
	defer overallRows.Close()

	overallStats := make(map[string]float64)
	for overallRows.Next() {
		var metric string
		var total, avg, max, min float64
		var count int64
		if err := overallRows.Scan(&metric, &total, &avg, &max, &min, &count); err != nil {
			return nil, nil, err
		}
		switch metric {
		case "success":
			overallStats["success"] = total
		case "failure":
			overallStats["failure"] = total
		case "latency_ms":
			overallStats["avg_latency"] = avg
			overallStats["max_latency"] = max
			overallStats["min_latency"] = min
			overallStats["latency_count"] = float64(count)
		}
	}

	// パスごとの統計を取得
	pathQuery := `
		SELECT 
			CASE 
				WHEN json_extract_path_text(labels, '/path') IS NULL OR json_extract_path_text(labels, '/path') = '' 
				THEN 'unknown' 
				ELSE json_extract_path_text(labels, '/path') 
			END as extracted_path,
			metric,
			SUM(value) as total,
			AVG(value) as avg,
			MAX(value) as max,
			MIN(value) as min
		FROM metrics
		WHERE timestamp >= ? AND timestamp <= ?
	`
	var pathArgs []any
	pathArgs = append(pathArgs, start, end)

	for k, v := range labels {
		pathQuery += " AND json_extract_path_text(labels, ?) = ?"
		pathArgs = append(pathArgs, "/"+k, v)
	}
	pathQuery += " GROUP BY extracted_path, metric"

	pathRows, err := d.db.QueryContext(ctx, pathQuery, pathArgs...)
	if err != nil {
		return nil, nil, err
	}
	defer pathRows.Close()

	pathMetrics := make(map[string]map[string]float64)
	for pathRows.Next() {
		var path, metric string
		var total, avg, max, min float64
		if err := pathRows.Scan(&path, &metric, &total, &avg, &max, &min); err != nil {
			return nil, nil, err
		}
		if _, ok := pathMetrics[path]; !ok {
			pathMetrics[path] = make(map[string]float64)
		}
		switch metric {
		case "success":
			pathMetrics[path]["success"] = total
		case "failure":
			pathMetrics[path]["failure"] = total
		case "latency_ms":
			pathMetrics[path]["avg_latency"] = avg
			pathMetrics[path]["max_latency"] = max
			pathMetrics[path]["min_latency"] = min
		}
	}

	return overallStats, pathMetrics, nil
}

func (d *duckDBDAO) Close() error {
	return d.db.Close()
}
