package cli

import (
	"log/slog"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/yuki-eto/swarun/pkg/config"
)

func TestGracefulShutdown(t *testing.T) {
	// ログ出力を抑制
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := config.DefaultConfig()
	// ポートが衝突しないように適当な値を設定（実際にはListenされない可能性もあるが）
	cfg.Port = 0

	// 別のゴルーチンでコントローラーを起動
	done := make(chan struct{})
	go func() {
		defer close(done)
		// runController はシグナルを待つので、ここでブロックされる
		runController(cfg, logger)
	}()

	// 少し待ってからシグナルを送信
	time.Sleep(100 * time.Millisecond)

	// SIGTERM を送信
	p, _ := os.FindProcess(os.Getpid())
	err := p.Signal(syscall.SIGTERM)
	if err != nil {
		t.Fatalf("Failed to send signal: %v", err)
	}

	// タイムアウト付きで終了を待機
	select {
	case <-done:
		// 正常終了
	case <-time.After(5 * time.Second):
		t.Fatal("Controller did not shut down in time")
	}
}
