package controller

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/pkg/config"
)

func TestController_TeardownWorkersClearsList(t *testing.T) {
	ctx := context.Background()
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	c, err := NewController(nil, cfg)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	defer c.Close()

	// 1. ワーカーを登録する
	workerID := "worker-1"
	_, err = c.RegisterWorker(ctx, connect.NewRequest(&swarunv1.RegisterWorkerRequest{
		WorkerId: workerID,
		Hostname: "localhost",
		Address:  "http://localhost:8081",
	}))
	if err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}

	// ワーカーが登録されていることを確認
	workers := c.workers.Load()
	if len(workers) != 1 {
		t.Fatalf("expected 1 worker, got %d", len(workers))
	}
	if _, ok := workers[workerID]; !ok {
		t.Fatalf("worker %s not found in list", workerID)
	}

	// 2. TeardownWorkers を呼び出す
	_, err = c.TeardownWorkers(ctx, connect.NewRequest(&swarunv1.TeardownWorkersRequest{}))
	if err != nil {
		t.Fatalf("failed to teardown workers: %v", err)
	}

	// 3. ワーカーリストが空になっていることを確認
	workers = c.workers.Load()
	if len(workers) != 0 {
		t.Errorf("expected 0 workers after teardown, got %d", len(workers))
	}
}
