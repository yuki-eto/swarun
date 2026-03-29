package logging

import (
	"log/slog"
	"os"
	"runtime"
)

// Setup はログレベルを指定してグローバルなロガーを設定します。
// levelStr は "debug", "info", "warn", "error" のいずれかです。
// エラー時にスタックトレースを含めるように設定します。
func Setup(levelStr string) *slog.Logger {
	level := parseLevel(levelStr)

	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	}

	var handler slog.Handler
	if os.Getenv("SWARUN_LOG_FORMAT") == "text" {
		handler = slog.NewTextHandler(os.Stderr, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

// New は特定のレベルで新しいロガーインスタンスを生成します（グローバル設定は変更しません）。
func New(levelStr string) *slog.Logger {
	level := parseLevel(levelStr)

	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	}

	var handler slog.Handler
	if os.Getenv("SWARUN_LOG_FORMAT") == "text" {
		handler = slog.NewTextHandler(os.Stderr, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	}
	return slog.New(handler)
}

func parseLevel(levelStr string) slog.Level {
	switch levelStr {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// ErrorAttr はエラーとそのスタックトレースを含む slog.Attr を返します。
func ErrorAttr(err error) slog.Attr {
	buf := make([]byte, 1024)
	n := runtime.Stack(buf, false)
	return slog.Group("error",
		slog.String("message", err.Error()),
		slog.String("stacktrace", string(buf[:n])),
	)
}
