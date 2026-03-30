package orchestrator

import (
	"context"
	"fmt"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/google/uuid"
	"github.com/moby/moby/client"
	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
)

func (o *Orchestrator) provisionDocker(ctx context.Context, mode *swarunv1.DockerMode, count int32, controllerAddr string) ([]string, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %w", err)
	}
	defer cli.Close()

	var ids []string
	for i := range int(count) {
		shortUUID := uuid.New().String()[:8]
		id := fmt.Sprintf("docker-worker-%s", shortUUID)

		// Docker ネットワーク内で実行されている場合を考慮し、デフォルトのコントローラーアドレスを調整
		addr := controllerAddr
		if addr == "" && o.cfg != nil {
			// controller サービス名で解決できるようにする
			addr = o.cfg.ControllerAddr
		}
		if addr == "" || addr == "http://localhost:8080" {
			addr = "http://controller:8080"
		}

		config := &container.Config{
			Image: mode.Image,
			Env: []string{
				fmt.Sprintf("SWARUN_WORKER_ID=%s", id),
				fmt.Sprintf("SWARUN_CONTROLLER_ADDR=%s", addr),
			},
			Cmd: []string{"-mode", "worker"},
		}

		hostConfig := &container.HostConfig{
			AutoRemove:  true,
			NetworkMode: "swarun_default",
		}

		networkingConfig := &network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{
				"swarun_default": {},
			},
		}

		resp, err := cli.ContainerCreate(ctx, config, hostConfig, networkingConfig, nil, id)
		if err != nil {
			return ids, fmt.Errorf("failed to create docker container %d: %w", i, err)
		}

		if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
			return ids, fmt.Errorf("failed to start docker container %d: %w", i, err)
		}

		o.dockerContainers[id] = resp.ID
		ids = append(ids, id)
		o.logger.Info("Provisioned docker worker", "id", id, "container_id", resp.ID)
	}
	return ids, nil
}
