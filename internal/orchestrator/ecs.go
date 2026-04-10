package orchestrator

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/google/uuid"
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

	var ids []string
	// SWARUN_WORKER_ID を別々に割り振らないといけないので RunTaskInput.Count は使わない
	for i := int32(0); i < count; i++ {
		input := &ecs.RunTaskInput{
			Cluster:        aws.String(mode.Cluster),
			TaskDefinition: aws.String(mode.TaskDefinition),
			Count:          aws.Int32(1),
			LaunchType:     types.LaunchTypeFargate,
			NetworkConfiguration: &types.NetworkConfiguration{
				AwsvpcConfiguration: &types.AwsVpcConfiguration{
					Subnets:        subnets,
					SecurityGroups: securityGroups,
					AssignPublicIp: types.AssignPublicIpEnabled,
				},
			},
			EnableExecuteCommand: true,
			Overrides: &types.TaskOverride{
				Cpu:    aws.String("1024"),
				Memory: aws.String("2048"),
				ContainerOverrides: []types.ContainerOverride{
					{
						Name: aws.String("swarun-app"),
						Command: []string{
							"-mode", "worker",
						},
						Environment: []types.KeyValuePair{
							{
								Name:  aws.String("SWARUN_WORKER_ID"),
								Value: aws.String("ecs-worker-" + uuid.NewString()),
							},
						}},
				},
			},
		}

		output, err := client.RunTask(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to run ECS task: %w", err)
		}

		task := output.Tasks[0]
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
