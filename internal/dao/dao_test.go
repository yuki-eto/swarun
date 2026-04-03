package dao

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestDuckDBPerTestID(t *testing.T) {
	dataDir := "testdata/dao"
	defer os.RemoveAll("testdata")

	testID1 := "test-run-1"
	dao1, err := NewDuckDBDAO(dataDir, testID1, true)
	if err != nil {
		t.Fatalf("failed to create dao1: %v", err)
	}
	defer dao1.Close()

	testID2 := "test-run-2"
	dao2, err := NewDuckDBDAO(dataDir, testID2, true)
	if err != nil {
		t.Fatalf("failed to create dao2: %v", err)
	}
	defer dao2.Close()

	ctx := context.Background()
	now := time.Now()

	// dao1 に挿入
	err = dao1.InsertRows(ctx, []Row{
		{Metric: "success", Value: 1, Timestamp: now},
	})
	if err != nil {
		t.Fatalf("failed to insert to dao1: %v", err)
	}

	// dao2 に挿入
	err = dao2.InsertRows(ctx, []Row{
		{Metric: "success", Value: 2, Timestamp: now},
	})
	if err != nil {
		t.Fatalf("failed to insert to dao2: %v", err)
	}

	// dao1 から取得
	rows1, err := dao1.SelectRows(ctx, "success", nil, now.Add(-time.Minute), now.Add(time.Minute), "", 0)
	if err != nil {
		t.Fatalf("failed to select from dao1: %v", err)
	}
	if len(rows1) != 1 || rows1[0].Value != 1 {
		t.Errorf("expected value 1 from dao1, got %v", rows1)
	}

	// dao2 から取得
	rows2, err := dao2.SelectRows(ctx, "success", nil, now.Add(-time.Minute), now.Add(time.Minute), "", 0)
	if err != nil {
		t.Fatalf("failed to select from dao2: %v", err)
	}
	if len(rows2) != 1 || rows2[0].Value != 2 {
		t.Errorf("expected value 2 from dao2, got %v", rows2)
	}
}
