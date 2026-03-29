package controller

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yuki-eto/swarun/pkg/config"
)

func TestController_Persistence(t *testing.T) {
	dataDir := "testdata/persistence"
	os.RemoveAll(dataDir)
	defer os.RemoveAll(dataDir)

	cfg := config.DefaultConfig()
	cfg.DataDir = dataDir

	// 1. コントローラーを作成
	c, err := NewController(nil, cfg)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}

	// 2. テスト実行を追加
	testID1 := "test-1"
	startTime1 := time.Now().Add(-time.Hour).Truncate(time.Second)
	endTime1 := startTime1.Add(time.Minute)
	tr1_orig := &TestRun{
		ID:           testID1,
		StartTime:    startTime1,
		EndTime:      endTime1,
		TotalSuccess: 100,
		TotalFailure: 5,
		IsRunning:    false,
		PathMetrics:  NewPathMetricsMap(),
	}
	tr1_orig.PathMetrics.Metrics["/api/v1"] = &PathStats{
		Success: 50,
		Failure: 2,
	}
	c.testRuns.Store(testID1, tr1_orig)
	if err := c.saveTestRuns(); err != nil {
		t.Fatalf("failed to save test runs: %v", err)
	}

	testID2 := "test-2"
	c.testRuns.Store(testID2, &TestRun{ID: testID2, IsRunning: true})
	if err := c.saveTestRuns(); err != nil {
		t.Fatalf("failed to save test runs: %v", err)
	}

	// 3. ファイルが存在し、中身が正しいか確認
	runsPath := filepath.Join(dataDir, "runs.json")
	if _, err := os.Stat(runsPath); os.IsNotExist(err) {
		t.Fatal("runs.json does not exist")
	}

	data, _ := os.ReadFile(runsPath)
	var runs []*TestRun
	if err := json.Unmarshal(data, &runs); err != nil {
		t.Fatalf("failed to unmarshal runs.json: %v", err)
	}
	if len(runs) != 2 {
		t.Errorf("expected 2 runs, got %d", len(runs))
	}

	// 4. 新しいコントローラーで読み込めるか確認
	c2, err := NewController(nil, cfg)
	if err != nil {
		t.Fatalf("failed to create second controller: %v", err)
	}

	tr1, ok := c2.testRuns.Get(testID1)
	if !ok {
		t.Errorf("testID1 not found in second controller")
	} else {
		if !tr1.StartTime.Equal(startTime1) {
			t.Errorf("expected StartTime %v, got %v", startTime1, tr1.StartTime)
		}
		if tr1.TotalSuccess != 100 {
			t.Errorf("expected TotalSuccess 100, got %d", tr1.TotalSuccess)
		}
		if tr1.TotalFailure != 5 {
			t.Errorf("expected TotalFailure 5, got %d", tr1.TotalFailure)
		}
		if tr1.PathMetrics == nil {
			t.Errorf("expected PathMetrics to be not nil")
		} else {
			if stats, ok := tr1.PathMetrics.Metrics["/api/v1"]; ok {
				if stats.Success != 50 {
					t.Errorf("expected PathMetrics Success 50, got %d", stats.Success)
				}
				if stats.Failure != 2 {
					t.Errorf("expected PathMetrics Failure 2, got %d", stats.Failure)
				}
			} else {
				t.Errorf("expected path /api/v1 to exist in PathMetrics")
			}
		}
	}

	tr2, ok := c2.testRuns.Get(testID2)
	if !ok {
		t.Errorf("testID2 not found in second controller")
	} else {
		if tr2.IsRunning {
			t.Errorf("expected IsRunning false after restart, got true")
		}
	}
}
