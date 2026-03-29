package controller

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"
	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/pkg/config"
)

func TestController_ExportData(t *testing.T) {
	dataDir, err := os.MkdirTemp("", "swarun-data-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dataDir)

	// Create dummy data
	files := map[string]string{
		"runs.json":      `[{"id": "test-1"}]`,
		"test-1/metrics": "dummy",
	}
	for name, content := range files {
		path := filepath.Join(dataDir, name)
		os.MkdirAll(filepath.Dir(path), 0755)
		os.WriteFile(path, []byte(content), 0644)
	}

	cfg := &config.Config{DataDir: dataDir}
	c, _ := NewController(nil, cfg)

	// Mock stream
	stream := &mockExportStream{
		buffer: new(bytes.Buffer),
	}

	err = c.exportData(context.Background(), stream)
	if err != nil {
		t.Fatalf("ExportData failed: %v", err)
	}

	// Verify zip content
	zr, err := zip.NewReader(bytes.NewReader(stream.buffer.Bytes()), int64(stream.buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}

	found := make(map[string]bool)
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		found[f.Name] = true
		expectedContent, ok := files[f.Name]
		if !ok {
			continue
		}
		rc, _ := f.Open()
		content, _ := io.ReadAll(rc)
		rc.Close()
		if string(content) != expectedContent {
			t.Errorf("file %s content mismatch: expected %s, got %s", f.Name, expectedContent, string(content))
		}
	}

	if !found["runs.json"] {
		t.Errorf("runs.json not found in zip")
	}
}

type mockExportStream struct {
	buffer *bytes.Buffer
}

func (m *mockExportStream) Send(msg *swarunv1.ExportDataResponse) error {
	m.buffer.Write(msg.Chunk)
	return nil
}

func (m *mockExportStream) Conn() connect.StreamingHandlerConn { return nil }
func (m *mockExportStream) ResponseHeader() http.Header        { return nil }
func (m *mockExportStream) ResponseTrailer() http.Header       { return nil }
