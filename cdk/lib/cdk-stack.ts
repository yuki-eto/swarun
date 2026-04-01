import * as cdk from "aws-cdk-lib/core";
import {Construct} from "constructs";
import {Peer, Port, SecurityGroup, SubnetType, Vpc} from "aws-cdk-lib/aws-ec2";
import {Repository} from "aws-cdk-lib/aws-ecr";
import {Cluster, ContainerInsights} from "aws-cdk-lib/aws-ecs";
import {ManagedPolicy, Role, ServicePrincipal} from "aws-cdk-lib/aws-iam";
import {BlockPublicAccess, Bucket} from "aws-cdk-lib/aws-s3";

export class CdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const vpc = new Vpc(this, "Vpc", {
      natGateways: 1,
      maxAzs: 2,
      subnetConfiguration: [
        {
          cidrMask: 24,
          name: "public-network",
          subnetType: SubnetType.PUBLIC,
        },
        {
          cidrMask: 16,
          name: "private-network",
          subnetType: SubnetType.PRIVATE_WITH_EGRESS,
        },
      ],
    });

    const ecrRepository = new Repository(this, "Repository", {
      repositoryName: "swarun",
    });

    const bucket = new Bucket(this, "Bucket", {
      bucketName: "swarun-results",
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
    });

    const ecsCluster = new Cluster(this, "Cluster", {
      vpc: vpc,
      clusterName: "swarun",
      containerInsightsV2: ContainerInsights.ENABLED,
    });

    const taskExecRole = new Role(this, "TaskExecRole", {
      roleName: "swarun-task-exec-role",
      assumedBy: new ServicePrincipal("ecs-tasks.amazonaws.com"),
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonECSTaskExecutionRolePolicy"),
      ],
    });

    const taskRole = new Role(this, "TaskRole", {
      roleName: "swarun-task-role",
      assumedBy: new ServicePrincipal("ecs-tasks.amazonaws.com"),
    });
    bucket.grantReadWrite(taskRole);

    const ecsSecurityGroup = new SecurityGroup(this, "ECSSecurityGroup", {
      vpc: vpc,
    });
    ecsSecurityGroup.addEgressRule(Peer.anyIpv4(), Port.tcp(80), "Allow HTTP traffic");
    ecsSecurityGroup.addEgressRule(Peer.anyIpv4(), Port.tcp(443), "Allow HTTPS traffic");
    ecsSecurityGroup.addEgressRule(ecsSecurityGroup, Port.allTraffic(), "Allow all traffic within the security group");
    ecsSecurityGroup.addIngressRule(ecsSecurityGroup, Port.allTraffic(), "Allow all traffic within the security group");
  }
}
