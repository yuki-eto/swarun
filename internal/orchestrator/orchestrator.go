package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"

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
	localProcesses   []*os.Process
	dockerContainers []string
	ecsTasks         []ecsTaskInfo
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
		logger:   logger,
		cfg:      cfg,
		platform: platform,
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
	for _, p := range o.localProcesses {
		if err := p.Kill(); err != nil {
			errs = append(errs, fmt.Sprintf("failed to kill local process %d: %v", p.Pid, err))
		} else {
			o.logger.Info("Teared down local worker", "pid", p.Pid)
		}
	}
	o.localProcesses = nil

	// Docker
	for _, id := range o.dockerContainers {
		cmd := exec.CommandContext(ctx, "docker", "rm", "-f", id)
		if out, err := cmd.CombinedOutput(); err != nil {
			errs = append(errs, fmt.Sprintf("failed to remove docker container %s: %v (output: %s)", id, err, string(out)))
		} else {
			o.logger.Info("Teared down docker worker", "id", id)
		}
	}
	o.dockerContainers = nil

	// ECS
	// region ごとに client を作る必要があるかもしれないが、一旦シンプルに
	for _, info := range o.ecsTasks {
		cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(info.region))
		if err != nil {
			errs = append(errs, fmt.Sprintf("failed to load AWS config for teardown (%s): %v", info.region, err))
			continue
		}
		client := ecs.NewFromConfig(cfg)
		_, err = client.StopTask(ctx, &ecs.StopTaskInput{
			Cluster: aws.String(info.cluster),
			Task:    aws.String(info.taskARN),
			Reason:  aws.String("Teardown requested by swarun controller"),
		})
		if err != nil {
			errs = append(errs, fmt.Sprintf("failed to stop ECS task %s: %v", info.taskARN, err))
		} else {
			o.logger.Info("Teared down ECS worker", "arn", info.taskARN)
		}
	}
	o.ecsTasks = nil

	if len(errs) > 0 {
		return fmt.Errorf("teardown errors: %s", strings.Join(errs, "; "))
	}
	return nil
}
