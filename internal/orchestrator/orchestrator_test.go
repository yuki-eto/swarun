package orchestrator

import (
	"testing"

	"github.com/yuki-eto/swarun/pkg/config"
)

func TestOrchestrator_PlatformSelection(t *testing.T) {
	tests := []struct {
		name     string
		platform string
	}{
		{
			name:     "local",
			platform: "local",
		},
		{
			name:     "docker",
			platform: "docker",
		},
		{
			name:     "ecs",
			platform: "ecs",
		},
		{
			name:     "empty defaults to local",
			platform: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.Platform = tt.platform
			o := NewOrchestrator(nil, cfg)

			expected := tt.platform
			if tt.platform == "" {
				expected = "local"
			}

			if o.platform != expected {
				t.Errorf("expected platform %s, got %s", expected, o.platform)
			}
		})
	}
}
