package dao

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestDuckDBDAO(t *testing.T) {
	dataDir := "testdata/duckdb"
	testRunID := "test-run-1"
	defer os.RemoveAll("testdata")

	dao, err := NewDuckDBDAO(dataDir, testRunID)
	if err != nil {
		t.Fatalf("failed to create DuckDB DAO: %v", err)
	}
	defer dao.Close()

	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)

	rows := []Row{
		{
			Metric:    "latency_ms",
			Value:     100,
			Timestamp: now.Add(-10 * time.Second),
			WorkerID:  "w1",
			Path:      "/api/v1",
			Labels:    map[string]string{"env": "test"},
		},
		{
			Metric:    "latency_ms",
			Value:     200,
			Timestamp: now.Add(-5 * time.Second),
			WorkerID:  "w1",
			Path:      "/api/v1",
			Labels:    map[string]string{"env": "test"},
		},
		{
			Metric:    "latency_ms",
			Value:     300,
			Timestamp: now,
			WorkerID:  "w1",
			Path:      "/api/v1",
			Labels:    map[string]string{"env": "test"},
		},
	}

	if err := dao.InsertRows(ctx, rows); err != nil {
		t.Fatalf("failed to insert rows: %v", err)
	}

	t.Run("SelectAll", func(t *testing.T) {
		res, err := dao.SelectRows(ctx, "latency_ms", map[string]string{"path": "/api/v1", "worker_id": "w1"}, now.Add(-20*time.Second), now.Add(time.Second), "", 0)
		if err != nil {
			t.Fatalf("failed to select rows: %v", err)
		}
		if len(res) != 3 {
			t.Errorf("expected 3 rows, got %d", len(res))
		}
	})

	t.Run("QueryRaw", func(t *testing.T) {
		res, err := dao.(*duckDBDAO).QueryRaw(ctx, "SELECT metric, value, labels, path, worker_id FROM metrics ORDER BY timestamp")
		if err != nil {
			t.Fatalf("failed to query raw: %v", err)
		}
		if len(res) != 3 {
			t.Errorf("expected 3 rows, got %d", len(res))
		}
		// labels がオブジェクトとして取得できているか確認
		m := res[0]
		labels, ok := m["labels"].(map[string]any)
		if !ok {
			t.Errorf("labels is not a map: %T", m["labels"])
		} else if labels["env"] != "test" {
			t.Errorf("expected env test, got %v", labels["env"])
		}
		if m["path"] != "/api/v1" {
			t.Errorf("expected path /api/v1, got %v", m["path"])
		}
		if m["worker_id"] != "w1" {
			t.Errorf("expected worker_id w1, got %v", m["worker_id"])
		}
	})

	t.Run("AggregateMean", func(t *testing.T) {
		// すべて一つの 30秒ウィンドウに入るように、十分長いウィンドウを指定
		// または開始時刻をウィンドウの境界に合わせる
		// DuckDB の time_bucket は Unix epoch (1970-01-01) 起点なので、ウィンドウサイズより大きく離れた時刻だと境界に注意が必要
		res, err := dao.SelectRows(ctx, "latency_ms", map[string]string{"path": "/api/v1", "worker_id": "w1"}, now.Add(-20*time.Second), now.Add(time.Second), "mean", 1*time.Hour)
		if err != nil {
			t.Fatalf("failed to aggregate rows: %v", err)
		}
		if len(res) != 1 {
			t.Errorf("expected 1 row for aggregate, got %d. rows: %+v", len(res), res)
		} else {
			expected := (100.0 + 200.0 + 300.0) / 3.0
			if res[0].Value != expected {
				t.Errorf("expected mean %f, got %f", expected, res[0].Value)
			}
		}
	})

	t.Run("AggregateWithWindow", func(t *testing.T) {
		// 5秒ウィンドウで集計
		// 10s前(100), 5s前(200), 0s(300)
		// ウィンドウ境界によっては分かれる
		res, err := dao.SelectRows(ctx, "latency_ms", map[string]string{"path": "/api/v1", "worker_id": "w1"}, now.Add(-20*time.Second), now.Add(time.Second), "sum", 2*time.Second)
		if err != nil {
			t.Fatalf("failed to aggregate rows: %v", err)
		}
		// 3点バラバラのウィンドウに入るはず（2秒間隔なので）
		if len(res) != 3 {
			t.Errorf("expected 3 rows for aggregate with small window, got %d", len(res))
		}
	})

	t.Run("SelectStats", func(t *testing.T) {
		// 追加のデータを投入
		rows := []Row{
			{
				Metric:    "success",
				Value:     1.0,
				Timestamp: now,
				WorkerID:  "w1",
				Path:      "/api/v1",
			},
			{
				Metric:    "success",
				Value:     1.0,
				Timestamp: now,
				WorkerID:  "w2",
				Path:      "/api/v2",
			},
			{
				Metric:    "failure",
				Value:     1.0,
				Timestamp: now,
				WorkerID:  "w1",
				Path:      "/api/v1",
			},
		}
		if err := dao.InsertRows(ctx, rows); err != nil {
			t.Fatalf("failed to insert rows: %v", err)
		}

		overall, pathStats, err := dao.SelectStats(ctx, nil, now.Add(-20*time.Second), now.Add(time.Second))
		if err != nil {
			t.Fatalf("failed to select stats: %v", err)
		}

		if overall["success"] != 2.0 {
			t.Errorf("expected overall success 2.0, got %v", overall["success"])
		}
		if overall["failure"] != 1.0 {
			t.Errorf("expected overall failure 1.0, got %v", overall["failure"])
		}

		if len(pathStats) != 2 {
			t.Errorf("expected 2 paths, got %d. pathStats: %+v", len(pathStats), pathStats)
		}

		// Labels なしのデータを追加して unknown として集計されるか確認
		rowsUnknown := []Row{
			{
				Metric:    "success",
				Value:     1.0,
				Timestamp: now,
				Labels:    nil,
			},
		}
		if err := dao.InsertRows(ctx, rowsUnknown); err != nil {
			t.Fatalf("failed to insert unknown rows: %v", err)
		}

		overall2, pathStats2, err := dao.SelectStats(ctx, nil, now.Add(-20*time.Second), now.Add(time.Second))
		if err != nil {
			t.Fatalf("failed to select stats 2: %v", err)
		}

		if overall2["success"] != 3.0 {
			t.Errorf("expected overall success 3.0, got %v", overall2["success"])
		}

		if stats, ok := pathStats2["unknown"]; ok {
			if stats["success"] != 1.0 {
				t.Errorf("expected unknown success 1.0, got %v", stats["success"])
			}
		} else {
			t.Errorf("unknown not found in pathStats2: %+v", pathStats2)
		}

		if stats, ok := pathStats["/api/v1"]; ok {
			if stats["success"] != 1.0 {
				t.Errorf("expected /api/v1 success 1.0, got %v", stats["success"])
			}
			if stats["failure"] != 1.0 {
				t.Errorf("expected /api/v1 failure 1.0, got %v", stats["failure"])
			}
		} else {
			t.Errorf("/api/v1 not found in pathStats")
		}

		if stats, ok := pathStats["/api/v2"]; ok {
			if stats["success"] != 1.0 {
				t.Errorf("expected /api/v2 success 1.0, got %v", stats["success"])
			}
		} else {
			t.Errorf("/api/v2 not found in pathStats")
		}
		t.Run("SelectWithWorkerID", func(t *testing.T) {
			workerRows := []Row{
				{
					Metric:    "latency_ms",
					Value:     150,
					Timestamp: now,
					WorkerID:  "worker-1",
					Path:      "/api/worker",
				},
			}
			if err := dao.InsertRows(ctx, workerRows); err != nil {
				t.Fatalf("failed to insert worker rows: %v", err)
			}

			res, err := dao.SelectRows(ctx, "latency_ms", map[string]string{"worker_id": "worker-1"}, now.Add(-time.Second), now.Add(time.Second), "", 0)
			if err != nil {
				t.Fatalf("failed to select by worker_id: %v", err)
			}
			if len(res) != 1 {
				t.Errorf("expected 1 row for worker-1, got %d", len(res))
			} else if res[0].Value != 150 {
				t.Errorf("expected value 150, got %f", res[0].Value)
			}
		})
	})
}
