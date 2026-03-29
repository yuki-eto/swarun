# swarun AWS ECS (Fargate) 構成ガイド

このディレクトリでは、`swarun` を AWS ECS (Fargate) 上で稼働させるための CDK テンプレートの設計方針と構成についてまとめています。

## 1. 全体構成のアーキテクチャ

*   **VPC**: Public Subnet (ALB/NAT Gateway 用) と Private Subnet (ECS サービス/タスク用) を作成。
*   **ECS Cluster**: Fargate を使用。
*   **Controller Service**:
    *   Fargate サービスとして実行。
    *   Private Subnet 内で実行し、必要に応じて ALB 経由でダッシュボードを公開。
    *   S3 への Read/Write 権限を持つ。
*   **Worker**:
    *   Controller からの指示により、オンデマンドで Fargate タスクとして起動。
    *   Controller と同じタスク定義を使用し、`command` の上書き（Command Override）によって起動モードを切り替える。
    *   Controller と通信可能なネットワーク設定（セキュリティグループ）が必要。

## 2. CDK 実装方針

### VPC & ネットワーク
*   **VPC**: `aws-ec2.Vpc` を使用。
*   **NAT Gateway**: 1つ以上配置し、Private Subnet からの外部疎通（S3, ECR, テスト対象へのアクセス）を確保。
*   **S3 Gateway Endpoint**: S3 通信のコスト削減と高速化のため追加。

### ECR & S3
*   **ECR**: `swarun` のコンテナイメージを格納。
*   **S3**: メトリクスや実行結果（`data/`）の永続化用（`SWARUN_S3_BUCKET`）。

### ECS Task Definition (共通化)
Controller と Worker で同じタスク定義を使用します。

*   **Image**: `swarun` のバイナリが含まれるイメージ。
*   **Task Role**: 以下の権限を付与。
    *   S3 バケットへの `s3:PutObject`, `s3:GetObject`, `s3:ListBucket`
    *   ECS の `ecs:RunTask` および `iam:PassRole`（Worker 起動用）
    *   CloudWatch Logs への書き込み
    *   ECS Exec 用の標準的な権限（SSM 関連）
*   **Environment Variables**:
    *   `SWARUN_S3_BUCKET`: 作成した S3 バケット名。
    *   `SWARUN_LAUNCH_MODE`: `ecs`。
    *   `SWARUN_ECS_CLUSTER`: Cluster ARN。
    *   `SWARUN_ECS_TASK_DEF`: 自分自身のタスク定義 ARN。
    *   `SWARUN_ECS_SUBNETS`: 起動先サブネットの ID（カンマ区切り）。
    *   `SWARUN_ECS_SG`: 起動時に付与するセキュリティグループ ID（カンマ区切り）。

### セキュリティグループ
*   **Controller SG**:
    *   Ingress: ポート `8080` を Worker SG および管理者用端末から許可。
*   **Worker SG**:
    *   Egress: ポート `8080` で Controller SG への通信を許可。
    *   Egress: HTTP/HTTPS (80/443) でテスト対象や AWS API への通信を許可。

## 3. データ永続化戦略 (EFS 不要)

EFS を使わず、S3 への `export`/`import` 機能を活用して再起動時のデータ復旧を実現します。

1.  **起動時 (Import)**:
    Controller 起動時に、`SWARUN_S3_BUCKET` から最新のデータを `import-s3` コマンドでローカルの `data/` ディレクトリに展開します。
2.  **運用・終了時 (Export)**:
    テスト完了時や定期的に `export-s3` コマンドを実行し、`data/runs.json` や DuckDB ファイルを S3 に同期します。

## 4. 運用フロー

1.  **デプロイ**: CDK で各リソース（VPC, S3, ECR, TaskDef, Controller Service）をデプロイ。
2.  **イメージ Push**: ビルドした `swarun` イメージを ECR へ Push。
3.  **データ復旧**: Controller 起動時に S3 からデータを pull（初回は不要）。
4.  **テスト実行**:
    *   Controller のダッシュボードまたは CLI から `provision-workers` を実行。
    *   Controller が `RunTask` を発行し、同じ TaskDef をコマンド上書き（`-mode worker`）で起動。
5.  **結果保存**: テスト終了後、`export-s3` で結果を S3 へ保存。
