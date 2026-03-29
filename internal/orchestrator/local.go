package orchestrator

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
)

func (o *Orchestrator) provisionLocal(ctx context.Context, mode *swarunv1.LocalMode, count int32, controllerAddr string) ([]string, error) {
	var ids []string
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("failed to get executable path: %w", err)
	}
	for i := range int(count) {
		id := fmt.Sprintf("local-worker-%d", len(o.localProcesses)+1)
		cmd := exec.Command(executable,
			"-mode", "worker",
			"-id", id,
			"-controller", controllerAddr,
		)
		// ログ出力を継承しない場合は stdout/stderr を捨てるかファイルに吐く
		if err := cmd.Start(); err != nil {
			return ids, fmt.Errorf("failed to start local worker %d: %w", i, err)
		}
		o.localProcesses = append(o.localProcesses, cmd.Process)
		ids = append(ids, id)
		o.logger.Info("Provisioned local worker", "id", id, "pid", cmd.Process.Pid)
	}
	return ids, nil
}
