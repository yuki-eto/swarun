package client

import (
	"flag"
	"testing"
	"time"

	"github.com/yuki-eto/swarun/pkg/config"
)

func TestClientEnvConsideration(t *testing.T) {
	// 環境変数を設定
	t.Setenv("SWARUN_CONTROLLER_ADDR", "http://env-addr:8080")
	t.Setenv("SWARUN_CONCURRENCY", "99")
	t.Setenv("SWARUN_DURATION", "1m")
	t.Setenv("SWARUN_COMMAND", "run-test")

	// flag をリセット (テスト実行時の引数が混ざらないように)
	fs := flag.NewFlagSet("test", flag.ContinueOnError)

	var (
		controllerAddr string
		concurrency    int
		duration       int64
		command        string
	)
	fs.StringVar(&controllerAddr, "controller", "http://localhost:8080", "")
	fs.IntVar(&concurrency, "concurrency", 10, "")
	fs.Int64Var(&duration, "duration", 10, "")
	fs.StringVar(&command, "cmd", "list-workers", "")

	// 引数なしでパース (デフォルト値になるはず)
	fs.Parse([]string{})

	// Config をロード (環境変数が反映されるはず)
	cfg, err := config.Load(nil)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// 判定ロジックをテスト
	isFlagPassed := func(name string) bool {
		found := false
		fs.Visit(func(f *flag.Flag) {
			if f.Name == name {
				found = true
			}
		})
		return found
	}

	resControllerAddr := controllerAddr
	if !isFlagPassed("controller") {
		resControllerAddr = cfg.ControllerAddr
	}

	resConcurrency := concurrency
	if !isFlagPassed("concurrency") {
		resConcurrency = cfg.Concurrency
	}

	resDuration := time.Duration(duration) * time.Second
	if !isFlagPassed("duration") {
		resDuration = cfg.Duration
	}

	resCommand := command
	if !isFlagPassed("cmd") {
		resCommand = cfg.Command
	}

	if resControllerAddr != "http://env-addr:8080" {
		t.Errorf("expected controllerAddr from env, got %s", resControllerAddr)
	}
	if resConcurrency != 99 {
		t.Errorf("expected concurrency from env, got %d", resConcurrency)
	}
	if resDuration != time.Minute {
		t.Errorf("expected duration from env, got %v", resDuration)
	}
	if resCommand != "run-test" {
		t.Errorf("expected command from env, got %s", resCommand)
	}
}

func TestClientFlagOverrideEnv(t *testing.T) {
	// 環境変数を設定
	t.Setenv("SWARUN_CONTROLLER_ADDR", "http://env-addr:8080")

	// flag を設定してパース
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var controllerAddr string
	fs.StringVar(&controllerAddr, "controller", "http://localhost:8080", "")

	// 引数をシミュレート
	fs.Parse([]string{"-controller", "http://flag-addr:8080"})

	// Config をロード
	cfg, _ := config.Load(nil)

	isFlagPassed := func(name string) bool {
		found := false
		fs.Visit(func(f *flag.Flag) {
			if f.Name == name {
				found = true
			}
		})
		return found
	}

	resControllerAddr := controllerAddr
	if !isFlagPassed("controller") {
		resControllerAddr = cfg.ControllerAddr
	}

	if resControllerAddr != "http://flag-addr:8080" {
		t.Errorf("expected controllerAddr from flag to override env, got %s", resControllerAddr)
	}
}
