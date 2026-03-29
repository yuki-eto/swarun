package orchestrator

import (
	"context"
	"fmt"
	"os/exec"

	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
)

func (o *Orchestrator) provisionDocker(ctx context.Context, mode *swarunv1.DockerMode, count int32, controllerAddr string) ([]string, error) {
	var ids []string
	for i := range int(count) {
		id := fmt.Sprintf("docker-worker-%d", len(o.dockerContainers)+1)
		// docker run -d --name <id> <image> -mode worker -id <id> -controller <controllerAddr>
		args := []string{"run", "-d", "--name", id, mode.Image, "-mode", "worker", "-id", id, "-controller", controllerAddr}
		cmd := exec.CommandContext(ctx, "docker", args...)
		if out, err := cmd.CombinedOutput(); err != nil {
			return ids, fmt.Errorf("failed to start docker worker %d: %w (output: %s)", i, err, string(out))
		}
		o.dockerContainers = append(o.dockerContainers, id)
		ids = append(ids, id)
		o.logger.Info("Provisioned docker worker", "id", id)
	}
	return ids, nil
}
