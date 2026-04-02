package controller

import (
	"context"
	"fmt"

	"github.com/yuki-eto/swarun/internal/dao"
)

func (c *Controller) getStorage(testRunID string) (dao.MetricsDAO, error) {
	if testRunID == "" {
		testRunID = "default"
	}

	var daoErr error
	d := c.storages.GetOrCompute(testRunID, func() dao.MetricsDAO {
		var d dao.MetricsDAO

		switch c.cfg.MetricsBackend {
		case "influxdb":
			d, daoErr = dao.NewInfluxDBDAO(
				context.Background(),
				c.cfg.InfluxDBURL,
				c.cfg.InfluxDBToken,
				c.cfg.InfluxDBOrg,
				c.cfg.InfluxDBBucket,
				testRunID,
			)
		default:
			// Default to duckdb
			useInMemory := c.cfg.DuckDBInMemoryMode
			if tr, ok := c.testRuns.Get(testRunID); ok {
				// Only use in-memory if the test is currently running
				useInMemory = useInMemory && tr.IsRunning
			}
			d, daoErr = dao.NewDuckDBDAO(c.dataDir, testRunID, useInMemory)
		}

		if daoErr != nil {
			c.logger.Error("Failed to initialize metrics DAO", "backend", c.cfg.MetricsBackend, "test_run_id", testRunID, "error", daoErr)
			return nil
		}

		c.logger.Info("Initialized metrics DAO", "backend", c.cfg.MetricsBackend, "test_run_id", testRunID)
		return d
	})

	if daoErr != nil {
		return nil, daoErr
	}
	if d == nil {
		return nil, fmt.Errorf("failed to initialize metrics DAO (nil)")
	}

	return d, nil
}
