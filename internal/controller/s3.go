package controller

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
	"github.com/yuki-eto/swarun/pkg/logging"
)

func (c *Controller) ExportToS3(
	ctx context.Context,
	req *connect.Request[swarunv1.ExportToS3Request],
) (*connect.Response[swarunv1.ExportToS3Response], error) {
	bucket := req.Msg.GetS3Bucket()
	if bucket == "" {
		bucket = c.defaultS3Bucket
	}
	prefix := req.Msg.GetS3Prefix()
	if prefix == "" {
		prefix = c.defaultS3Prefix
	}
	region := req.Msg.GetS3Region()
	if region == "" {
		region = c.defaultS3Region
	}

	if bucket == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("s3_bucket is required"))
	}

	c.logger.Info("Exporting data to S3", "bucket", bucket, "prefix", prefix, "region", region, "test_run_id", req.Msg.GetTestRunId())

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to load AWS config: %w", err))
	}

	s3Client := s3.NewFromConfig(cfg)

	exportDir := c.dataDir
	if req.Msg.GetTestRunId() != "" {
		exportDir = filepath.Join(c.dataDir, req.Msg.GetTestRunId())
	}

	// exportDir 内のファイルを再帰的に検索してアップロード
	err = filepath.Walk(exportDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(c.dataDir, path)
		if err != nil {
			return err
		}

		key := filepath.Join(prefix, relPath)
		c.logger.Debug("Uploading file to S3", "path", path, "key", key)

		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", path, err)
		}
		defer f.Close()

		_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   f,
		})
		if err != nil {
			return fmt.Errorf("failed to upload %s to S3: %w", path, err)
		}

		return nil
	})

	if err != nil {
		c.logger.Error("Failed to export data to S3", logging.ErrorAttr(err))
		return connect.NewResponse(&swarunv1.ExportToS3Response{
			Success: false,
			Message: err.Error(),
		}), nil
	}

	return connect.NewResponse(&swarunv1.ExportToS3Response{
		Success: true,
		Message: "Data exported successfully",
	}), nil
}

func (c *Controller) ImportFromS3(
	ctx context.Context,
	req *connect.Request[swarunv1.ImportFromS3Request],
) (*connect.Response[swarunv1.ImportFromS3Response], error) {
	bucket := req.Msg.GetS3Bucket()
	if bucket == "" {
		bucket = c.defaultS3Bucket
	}
	prefix := req.Msg.GetS3Prefix()
	if prefix == "" {
		prefix = c.defaultS3Prefix
	}
	region := req.Msg.GetS3Region()
	if region == "" {
		region = c.defaultS3Region
	}

	if bucket == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("s3_bucket is required"))
	}

	testRunID := req.Msg.GetTestRunId()
	c.logger.Info("Importing data from S3", "bucket", bucket, "prefix", prefix, "region", region, "test_run_id", testRunID)

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to load AWS config: %w", err))
	}

	s3Client := s3.NewFromConfig(cfg)

	// prefix 内のオブジェクトをリスト
	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to list S3 objects: %w", err))
		}

		for _, obj := range page.Contents {
			relKey, err := filepath.Rel(prefix, *obj.Key)
			if err != nil {
				continue
			}

			destPath := filepath.Join(c.dataDir, relKey)
			if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to create directory %s: %w", filepath.Dir(destPath), err))
			}

			c.logger.Debug("Downloading file from S3", "key", *obj.Key, "dest", destPath)

			getObj, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
			})
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to get S3 object %s: %w", *obj.Key, err))
			}
			defer getObj.Body.Close()

			f, err := os.Create(destPath)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to create file %s: %w", destPath, err))
			}
			defer f.Close()

			if _, err := io.Copy(f, getObj.Body); err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to write file %s: %w", destPath, err))
			}
		}
	}

	return connect.NewResponse(&swarunv1.ImportFromS3Response{
		Success: true,
		Message: "Data imported successfully",
	}), nil
}
