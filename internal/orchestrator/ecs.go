package orchestrator

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
)

func (o *Orchestrator) provisionECS(ctx context.Context, mode *swarunv1.ECSMode, count int32, controllerAddr string) ([]string, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(mode.Region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	client := ecs.NewFromConfig(cfg)

	subnets := strings.Split(mode.Subnets, ",")
	securityGroups := strings.Split(mode.SecurityGroups, ",")

	input := &ecs.RunTaskInput{
		Cluster:        aws.String(mode.Cluster),
		TaskDefinition: aws.String(mode.TaskDefinition),
		Count:          aws.Int32(count),
		LaunchType:     types.LaunchTypeFargate,
		NetworkConfiguration: &types.NetworkConfiguration{
			AwsvpcConfiguration: &types.AwsVpcConfiguration{
				Subnets:        subnets,
				SecurityGroups: securityGroups,
				AssignPublicIp: types.AssignPublicIpEnabled,
			},
		},
		Overrides: &types.TaskOverride{
			ContainerOverrides: []types.ContainerOverride{
				{
					Name: aws.String("swarun-worker"), // タスク定義内のコンテナ名を想定
					Command: []string{
						"-mode", "worker",
						"-controller", controllerAddr,
					},
				},
			},
		},
	}

	output, err := client.RunTask(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to run ECS task: %w", err)
	}

	var ids []string
	for _, task := range output.Tasks {
		arn := aws.ToString(task.TaskArn)
		// worker_id は ARN をそのまま使う（RunTask で複数起動した場合、個別の ID を指定する手段が限られるため）
		o.ecsTasks[arn] = ecsTaskInfo{
			cluster: mode.Cluster,
			taskARN: arn,
			region:  mode.Region,
		}
		ids = append(ids, arn)
		o.logger.Info("Provisioned ECS task", "arn", arn)
	}

	return ids, nil
}
