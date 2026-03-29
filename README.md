# swarun

`swarun` は Go で書かれた、WebAPI などの負荷試験を分散実行するためのツールです。
ECS on Fargate や Docker、ローカル環境を動作環境とし、コントローラーとワーカーによる分散構成で大規模な負荷生成を可能にします。

## 特徴

- **分散アーキテクチャ**: 単一のバイナリでコントローラーとワーカーの両方の役割を担います（起動引数で切り替え）。また、単一プロセスで両方を動かすスタンドアローンモードもサポートしています。
- **柔軟なシナリオ定義**: 外部バイナリの実行ではなく、Go の `Scenario` インターフェースを実装することでテストロジックを定義します。
- **Connect RPC 通信**: コントローラーとワーカー間、およびクライアント間の通信には [Connect RPC](https://connectrpc.com/) を採用し、効率的で型安全な疎通を実現します。
- **豊富なメトリクス**:
  - Request per seconds (RPS)
  - 成功・失敗数
  - レイテンシ（応答時間、TTFB、リクエスト/レスポンスレイテンシ）
  - レスポンスサイズ
  - ステータスコード
  - カスタマイズ可能なメトリクス送信機能
- **ワーカープロビジョニング**: ローカルプロセス、Docker コンテナ、AWS ECS タスクとしてワーカーを動的に起動・停止（プロビジョニング）可能。
- **リアルタイムダッシュボード**: コントローラー上で動作するブラウザベースのダッシュボード（React 製）で、テスト状況をリアルタイムに確認。
- **メトリクスバックエンド**: デフォルトで DuckDB（埋め込み）を使用。InfluxDB への外部保存もサポート。
- **データ永続化**: テスト実行履歴やメトリクスをローカル（DuckDB）または InfluxDB に保持。S3 へのエクスポート・インポートも可能。
- **クライアントライブラリ**: `pkg/client` パッケージを使用して、Go プログラムから直接コントローラーを制御可能。

## アーキテクチャ

### コントローラー (Controller)
- ワーカーの起動管理（ローカル、Docker、ECS 対応）
- ワーカーの一覧とステータスの管理
- 負荷試験の一括開始命令
- テスト実行の進捗状況管理と集約
- S3 へのデータエクスポート・インポート機能
- Web ダッシュボードの提供

### ワーカー (Worker)
- 負荷試験の実行（Go で定義されたシナリオの実行）
- ワーカー内での並列実行数（同時実行プロセス数）の制御（段階的なランプアップを含む）
- 指定された時間または回数による実行制御
- メトリクスの計測とコントローラーへの送信

### クライアント (Client / CLI)
- コントローラーへのコマンド送信（テスト開始、ステータス確認、プロビジョニング等）
- リアルタイム監視（`watch-status`）
- メトリクスデータの取得

## 設定

`swarun` は環境変数、YAML 設定ファイル、およびコマンドライン引数で設定可能です。優先順位は以下の通りです：
1. コマンドライン引数（CLI 実行時）
2. 環境変数
3. YAML 設定ファイル
4. デフォルト値

### 主な環境変数

| 環境変数 | 説明 | デフォルト値 |
| :--- | :--- | :--- |
| `SWARUN_PORT` | コントローラーの待ち受けポート | `8080` |
| `SWARUN_CONTROLLER_ADDR` | コントローラーのアドレス | `http://localhost:8080` |
| `SWARUN_LOG_LEVEL` | ログレベル (`debug`, `info`, `warn`, `error`) | `info` |
| `SWARUN_DATA_DIR` | メトリクスデータや実行履歴の保存先 | `data` |
| `SWARUN_METRICS_BACKEND` | メトリクス保存先 (`duckdb`, `influxdb`) | `duckdb` |

### YAML 設定ファイルの使用

```bash
./tmp/swarun-example -config config.yaml
```

## クイックスタート

1. **ビルド**
   ```bash
   # 制御用 CLI のビルド
   go build -o tmp/swarun ./cmd/swarun/main.go

   # シナリオ（テスト内容）を含むバイナリのビルド
   go build -o tmp/swarun-example ./examples/simple-get/main.go
   ```

2. **コントローラー起動**
   ```bash
   ./tmp/swarun-example -mode controller
   ```

3. **ワーカーのプロビジョニング（例：ローカルで 3台起動）**
   別のターミナルで `swarun` CLI を使用します。
   ```bash
   ./tmp/swarun -cmd provision-workers -launch-mode local -worker-count 3
   ```

4. **テストの開始**
   ```bash
   ./tmp/swarun -cmd run-test -concurrency 10 -duration 30s
   ```
   ※ コマンド実行後に Test Run ID (UUID) が表示されます。

5. **テスト状況のリアルタイム監視**
   ```bash
   ./tmp/swarun -cmd watch-status -test-id <TEST_RUN_ID>
   ```

6. **Web ダッシュボードの確認**
   ブラウザで `http://localhost:8080` にアクセスすると、これまでのテスト実行履歴や詳細なメトリクスグラフを確認できます。

7. **ワーカーの停止（一括削除）**
   ```bash
   ./tmp/swarun -cmd teardown-workers
   ```

## スタンドアローンモード

コントローラー、ワーカーを同一プロセス内で起動し、テストを即時実行します。ローカルでのシナリオ開発や小規模なテストに便利です。
```bash
./tmp/swarun-example -mode standalone -concurrency 5 -duration 30s
```

## メトリクスバックエンド

### DuckDB (デフォルト)
ローカルの `SWARUN_DATA_DIR` にデータを保存します。外部データベースを必要とせず、高速な集計が可能です。

### InfluxDB
大規模なデータ保持や、既存の監視基盤との統合が必要な場合に使用します。以下の環境変数を設定してください。
- `SWARUN_METRICS_BACKEND`: `influxdb`
- `SWARUN_INFLUXDB_URL`, `SWARUN_INFLUXDB_TOKEN`, `SWARUN_INFLUXDB_ORG`, `SWARUN_INFLUXDB_BUCKET`
