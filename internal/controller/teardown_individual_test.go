package controller

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/pkg/config"
)

func TestController_TeardownWorker(t *testing.T) {
	ctx := context.Background()
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	c, err := NewController(nil, cfg)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	defer c.Close()

	// 1. ワーカーを2つ登録する
	workerID1 := "worker-1"
	workerID2 := "worker-2"

	_, _ = c.RegisterWorker(ctx, connect.NewRequest(&swarunv1.RegisterWorkerRequest{
		WorkerId: workerID1,
		Hostname: "host1",
		Address:  "http://localhost:8081",
	}))
	_, _ = c.RegisterWorker(ctx, connect.NewRequest(&swarunv1.RegisterWorkerRequest{
		WorkerId: workerID2,
		Hostname: "host2",
		Address:  "http://localhost:8082",
	}))

	// 2. worker-1 だけ個別に削除する
	resp, err := c.TeardownWorker(ctx, connect.NewRequest(&swarunv1.TeardownWorkerRequest{
		WorkerId: workerID1,
	}))
	if err != nil {
		t.Fatalf("failed to teardown worker: %v", err)
	}
	if !resp.Msg.Success {
		t.Errorf("expected success, got false")
	}

	// 3. worker-1 がリストから消え、worker-2 が残っていることを確認
	workers := c.workers.Load()
	if _, ok := workers[workerID1]; ok {
		t.Errorf("worker-1 should be removed from list")
	}
	if _, ok := workers[workerID2]; !ok {
		t.Errorf("worker-2 should still be in list")
	}
	if len(workers) != 1 {
		t.Errorf("expected 1 worker, got %d", len(workers))
	}
}
