package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/docker/docker/api/types/container"
	"github.com/moby/moby/client"
	"github.com/yuki-eto/swarun/pkg/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
)

type Launcher interface {
	Provision(ctx context.Context, req *swarunv1.ProvisionWorkersRequest) ([]string, error)
	Teardown(ctx context.Context) error
}

type Orchestrator struct {
	logger   *slog.Logger
	cfg      *config.Config
	platform string
	mu       sync.Mutex
	// 起動したプロセスやタスクを追跡するための情報
	localProcesses   map[string]*os.Process
	dockerContainers map[string]string      // key: worker_id, value: container_id or name
	ecsTasks         map[string]ecsTaskInfo // key: worker_id
}

type ecsTaskInfo struct {
	cluster string
	taskARN string
	region  string
}

func NewOrchestrator(logger *slog.Logger, cfg *config.Config) *Orchestrator {
	if logger == nil {
		logger = slog.Default()
	}
	platform := "local"
	if cfg != nil && cfg.Platform != "" {
		platform = cfg.Platform
	}
	return &Orchestrator{
		logger:           logger,
		cfg:              cfg,
		platform:         platform,
		localProcesses:   make(map[string]*os.Process),
		dockerContainers: make(map[string]string),
		ecsTasks:         make(map[string]ecsTaskInfo),
	}
}

func (o *Orchestrator) Provision(ctx context.Context, req *swarunv1.ProvisionWorkersRequest) ([]string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	mode := req.GetMode()
	if mode == nil {
		// リクエストでモードが指定されていない場合、プラットフォームに応じたデフォルト挙動をとる
		switch o.platform {
		case "docker":
			// Docker環境の場合はデフォルトのイメージを使用
			image := "swarun:latest"
			if o.cfg != nil && o.cfg.DockerImage != "" {
				image = o.cfg.DockerImage
			}
			mode = &swarunv1.ProvisionWorkersRequest_Docker{
				Docker: &swarunv1.DockerMode{Image: image},
			}
		case "ecs":
			// ECS環境の場合のデフォルト（設定から補完）
			ecsMode := &swarunv1.ECSMode{}
			if o.cfg != nil {
				ecsMode.Cluster = o.cfg.ECSCluster
				ecsMode.TaskDefinition = o.cfg.ECSTaskDef
				ecsMode.Region = o.cfg.ECSRegion
				ecsMode.Subnets = o.cfg.ECSSubnets
				ecsMode.SecurityGroups = o.cfg.ECSSG
			}
			mode = &swarunv1.ProvisionWorkersRequest_Ecs{
				Ecs: ecsMode,
			}
		default:
			mode = &swarunv1.ProvisionWorkersRequest_Local{
				Local: &swarunv1.LocalMode{},
			}
		}
	}

	switch m := mode.(type) {
	case *swarunv1.ProvisionWorkersRequest_Local:
		return o.provisionLocal(ctx, m.Local, req.Count, req.ControllerAddress)
	case *swarunv1.ProvisionWorkersRequest_Docker:
		return o.provisionDocker(ctx, m.Docker, req.Count, req.ControllerAddress)
	case *swarunv1.ProvisionWorkersRequest_Ecs:
		return o.provisionECS(ctx, m.Ecs, req.Count, req.ControllerAddress)
	default:
		return nil, fmt.Errorf("unsupported mode")
	}
}

func (o *Orchestrator) Teardown(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	var errs []string

	// Local
	for id, p := range o.localProcesses {
		if err := p.Kill(); err != nil {
			errs = append(errs, fmt.Sprintf("failed to kill local process %d (%s): %v", p.Pid, id, err))
		} else {
			o.logger.Info("Teared down local worker", "id", id, "pid", p.Pid)
		}
	}
	o.localProcesses = make(map[string]*os.Process)

	// Docker
	if len(o.dockerContainers) > 0 {
		cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
		if err == nil {
			defer cli.Close()
			for workerID, containerID := range o.dockerContainers {
				if err := cli.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true}); err != nil {
					errs = append(errs, fmt.Sprintf("failed to remove docker container %s (%s): %v", containerID, workerID, err))
				} else {
					o.logger.Info("Teared down docker worker", "worker_id", workerID, "container_id", containerID)
				}
			}
		} else {
			errs = append(errs, fmt.Sprintf("failed to create docker client for teardown: %v", err))
		}
	}
	o.dockerContainers = make(map[string]string)

	// ECS
	for workerID, info := range o.ecsTasks {
		cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(info.region))
		if err != nil {
			errs = append(errs, fmt.Sprintf("failed to load AWS config for teardown (%s, %s): %v", info.region, workerID, err))
			continue
		}
		client := ecs.NewFromConfig(cfg)
		_, err = client.StopTask(ctx, &ecs.StopTaskInput{
			Cluster: aws.String(info.cluster),
			Task:    aws.String(info.taskARN),
			Reason:  aws.String("Teardown requested by swarun controller"),
		})
		if err != nil {
			errs = append(errs, fmt.Sprintf("failed to stop ECS task %s (%s): %v", info.taskARN, workerID, err))
		} else {
			o.logger.Info("Teared down ECS worker", "worker_id", workerID, "arn", info.taskARN)
		}
	}
	o.ecsTasks = make(map[string]ecsTaskInfo)

	if len(errs) > 0 {
		return fmt.Errorf("teardown errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (o *Orchestrator) TeardownWorker(ctx context.Context, workerID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Local
	if p, ok := o.localProcesses[workerID]; ok {
		if err := p.Kill(); err != nil {
			return fmt.Errorf("failed to kill local process %d (%s): %w", p.Pid, workerID, err)
		}
		delete(o.localProcesses, workerID)
		o.logger.Info("Teared down local worker", "id", workerID, "pid", p.Pid)
		return nil
	}

	// Docker
	if containerID, ok := o.dockerContainers[workerID]; ok {
		cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
		if err != nil {
			return fmt.Errorf("failed to create docker client: %w", err)
		}
		defer cli.Close()
		if err := cli.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true}); err != nil {
			return fmt.Errorf("failed to remove docker container %s (%s): %w", containerID, workerID, err)
		}
		delete(o.dockerContainers, workerID)
		o.logger.Info("Teared down docker worker", "worker_id", workerID, "container_id", containerID)
		return nil
	}

	// ECS
	if info, ok := o.ecsTasks[workerID]; ok {
		cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(info.region))
		if err != nil {
			return fmt.Errorf("failed to load AWS config: %w", err)
		}
		client := ecs.NewFromConfig(cfg)
		_, err = client.StopTask(ctx, &ecs.StopTaskInput{
			Cluster: aws.String(info.cluster),
			Task:    aws.String(info.taskARN),
			Reason:  aws.String("Teardown requested by swarun controller"),
		})
		if err != nil {
			return fmt.Errorf("failed to stop ECS task %s (%s): %w", info.taskARN, workerID, err)
		}
		delete(o.ecsTasks, workerID)
		o.logger.Info("Teared down ECS worker", "worker_id", workerID, "arn", info.taskARN)
		return nil
	}

	return fmt.Errorf("worker not found in orchestrator: %s", workerID)
}
