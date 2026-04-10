package swarun

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDoLoggingOnFailure(t *testing.T) {
	// エラーを返すモックサーバー
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("error message"))
	}))
	defer server.Close()

	// swarun の初期化（本来はコントローラーのアドレスが必要だが、Doの実行自体はクライアントがあれば動くはず）
	// initEnv は内部で一回だけ実行されるように工夫されている

	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL, nil)
	resp, err := Do(req)
	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}

	// Body が読めることを確認（Do 内部で差し替えられているはず）
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if string(body) != "error message" {
		t.Errorf("expected body 'error message', got '%s'", string(body))
	}
}
