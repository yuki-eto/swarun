package client

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/gen/proto/v1/swarunv1connect"
)

// Client は Controller への高レベルなクライアントを提供します。
type Client struct {
	inner swarunv1connect.ControllerServiceClient
}

// NewClient は新しい Client を作成します。
func NewClient(addr string) *Client {
	return &Client{
		inner: swarunv1connect.NewControllerServiceClient(
			http.DefaultClient,
			addr,
		),
	}
}

// RunTest は負荷試験を開始します。
func (c *Client) RunTest(ctx context.Context, req *swarunv1.RunTestRequest) (*swarunv1.RunTestResponse, error) {
	resp, err := c.inner.RunTest(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// GetTestStatus はテストの進捗状況を取得します。
func (c *Client) GetTestStatus(ctx context.Context, testRunID string) (*swarunv1.GetTestStatusResponse, error) {
	resp, err := c.inner.GetTestStatus(ctx, connect.NewRequest(&swarunv1.GetTestStatusRequest{
		TestRunId: testRunID,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// ListWorkers は登録されているワーカーの一覧を取得します。
func (c *Client) ListWorkers(ctx context.Context) ([]*swarunv1.WorkerInfo, error) {
	resp, err := c.inner.ListWorkers(ctx, connect.NewRequest(&swarunv1.ListWorkersRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetWorkers(), nil
}

// ProvisionWorkers はワーカーをプロビジョニングします。
func (c *Client) ProvisionWorkers(ctx context.Context, req *swarunv1.ProvisionWorkersRequest) (*swarunv1.ProvisionWorkersResponse, error) {
	resp, err := c.inner.ProvisionWorkers(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// TeardownWorkers は全ワーカーを停止・削除します。
func (c *Client) TeardownWorkers(ctx context.Context) (*swarunv1.TeardownWorkersResponse, error) {
	resp, err := c.inner.TeardownWorkers(ctx, connect.NewRequest(&swarunv1.TeardownWorkersRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// TeardownWorker は特定のワーカーを停止・削除します。
func (c *Client) TeardownWorker(ctx context.Context, workerID string) (*swarunv1.TeardownWorkerResponse, error) {
	resp, err := c.inner.TeardownWorker(ctx, connect.NewRequest(&swarunv1.TeardownWorkerRequest{
		WorkerId: workerID,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// ExportToS3 はデータを S3 にエクスポートします。
func (c *Client) ExportToS3(ctx context.Context, req *swarunv1.ExportToS3Request) (*swarunv1.ExportToS3Response, error) {
	resp, err := c.inner.ExportToS3(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// ImportFromS3 はデータを S3 からインポートします。
func (c *Client) ImportFromS3(ctx context.Context, req *swarunv1.ImportFromS3Request) (*swarunv1.ImportFromS3Response, error) {
	resp, err := c.inner.ImportFromS3(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// GetMetrics はメトリクスを取得します。
func (c *Client) GetMetrics(ctx context.Context, req *swarunv1.GetMetricsRequest) (*swarunv1.GetMetricsResponse, error) {
	resp, err := c.inner.GetMetrics(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// ExportData は data_dir の内容を zip で固めて取得します。
func (c *Client) ExportData(ctx context.Context) (*connect.ServerStreamForClient[swarunv1.ExportDataResponse], error) {
	return c.inner.ExportData(ctx, connect.NewRequest(&swarunv1.ExportDataRequest{}))
}

// ImportData は受信した zip データをコントローラーへ送信します。
func (c *Client) ImportData(ctx context.Context) *connect.ClientStreamForClient[swarunv1.ImportDataRequest, swarunv1.ImportDataResponse] {
	return c.inner.ImportData(ctx)
}
