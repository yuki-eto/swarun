import {
  Box,
  Button,
  Card,
  CardContent,
  CircularProgress,
  Grid,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import {
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  TimeScale,
  Title,
  Tooltip,
} from "chart.js";
import { useCallback, useEffect, useState } from "react";
import { Line } from "react-chartjs-2";
import { useNavigate, useParams } from "react-router-dom";
import { client } from "../api/client";
import { getConfig } from "../config";
import type { GetTestStatusResponse, MetricData } from "../gen/swarun_pb";
import "chartjs-adapter-date-fns";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  TimeScale,
);

const TestRunDetail = () => {
  const config = getConfig();
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [status, setStatus] = useState<GetTestStatusResponse | null>(null);
  const [latencyMetrics, setLatencyMetrics] = useState<MetricData[]>([]);
  const [rpsMetrics, setRpsMetrics] = useState<MetricData[]>([]);
  const [loading, setLoading] = useState(true);
  const [stopping, setStopping] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [exportingS3, setExportingS3] = useState(false);

  const fetchStatus = useCallback(async () => {
    if (!id) return null;
    try {
      const resp = await client.getTestStatus({ testRunId: id });
      setStatus(resp);
      return resp;
    } catch (err) {
      console.error("Failed to fetch status:", err);
      return null;
    }
  }, [id]);

  const fetchMetrics = useCallback(async () => {
    if (!id) return;
    try {
      const [latencyResp, rpsResp] = await Promise.all([
        client.getMetrics({
          testRunId: id,
          metricName: "latency_ms",
          aggregateFunc: "mean",
          aggregateWindow: { seconds: BigInt(5), nanos: 0 },
        }),
        client.getMetrics({
          testRunId: id,
          metricName: "success",
          aggregateFunc: "sum",
          aggregateWindow: { seconds: BigInt(5), nanos: 0 },
        }),
      ]);
      setLatencyMetrics(latencyResp.points);
      setRpsMetrics(rpsResp.points);
    } catch (err) {
      console.error("Failed to fetch metrics:", err);
    }
  }, [id]);

  useEffect(() => {
    if (!id) return;

    let interval: number;

    const init = async () => {
      const [s] = await Promise.all([fetchStatus(), fetchMetrics()]);
      setLoading(false);

      if (s?.isRunning) {
        interval = setInterval(async () => {
          const [currentStatus] = await Promise.all([
            fetchStatus(),
            fetchMetrics(),
          ]);
          if (!currentStatus?.isRunning) {
            clearInterval(interval);
          }
        }, 2000);
      }
    };
    init();

    return () => {
      if (interval) clearInterval(interval);
    };
  }, [id, fetchStatus, fetchMetrics]);

  const handleStop = async () => {
    if (!id) return;
    setStopping(true);
    try {
      await client.stopTest({ testRunId: id });
      fetchStatus();
    } catch (err) {
      console.error("Failed to stop test:", err);
    } finally {
      setStopping(false);
    }
  };

  const handleExport = async () => {
    if (!id) return;
    setExporting(true);
    try {
      const resp = await client.exportReport({ testRunId: id });
      const blob = new Blob([resp.html], { type: "text/html" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `report-${id}.html`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (err) {
      console.error("Failed to export report:", err);
      alert("Failed to export report");
    } finally {
      setExporting(false);
    }
  };

  const handleExportToS3 = async () => {
    if (!id) return;
    setExportingS3(true);
    try {
      const resp = await client.exportToS3({ testRunId: id });
      if (resp.success) {
        alert("Exported to S3 successfully");
      } else {
        alert(`Export failed: ${resp.message}`);
      }
    } catch (err) {
      console.error("Failed to export to S3:", err);
      alert("Failed to export to S3");
    } finally {
      setExportingS3(false);
    }
  };

  const startTime = status?.startTime?.toDate();
  const endTime = status?.endTime?.toDate();
  const firstRequestTime = status?.firstRequestTime?.toDate();
  const lastRequestTime = status?.lastRequestTime?.toDate();
  const durationSec = status?.duration ? Number(status.duration.seconds) : 0;
  const expectedEndTime =
    startTime && durationSec > 0
      ? new Date(startTime.getTime() + durationSec * 1000)
      : null;

  // Calculate duration for RPS
  let actualDurationSec = status?.isRunning
    ? startTime
      ? Math.max(1, (Date.now() - startTime.getTime()) / 1000)
      : 0
    : startTime && endTime && !endTime.toISOString().startsWith("1970")
      ? Math.max(1, (endTime.getTime() - startTime.getTime()) / 1000)
      : durationSec || 1;

  if (
    firstRequestTime &&
    lastRequestTime &&
    lastRequestTime.getTime() > firstRequestTime.getTime()
  ) {
    actualDurationSec =
      (lastRequestTime.getTime() - firstRequestTime.getTime()) / 1000;
  } else if (rpsMetrics.length > 0) {
    const first = rpsMetrics[0].timestamp?.toDate();
    const last = rpsMetrics[rpsMetrics.length - 1].timestamp?.toDate();
    if (first && last && last.getTime() > first.getTime()) {
      actualDurationSec = (last.getTime() - first.getTime()) / 1000;
    }
  }

  const calculatedRps =
    (Number(status?.totalSuccess || 0) + Number(status?.totalFailure || 0)) /
    actualDurationSec;

  const [remainingSec, setRemainingSec] = useState<number | null>(null);

  useEffect(() => {
    if (status?.isRunning && expectedEndTime) {
      const timer = setInterval(() => {
        const now = new Date();
        const diff = Math.max(
          0,
          Math.floor((expectedEndTime.getTime() - now.getTime()) / 1000),
        );
        setRemainingSec(diff);
      }, 1000);
      return () => clearInterval(timer);
    } else {
      setRemainingSec(null);
    }
  }, [status?.isRunning, expectedEndTime]);

  if (loading) {
    return (
      <Box sx={{ display: "flex", justifyContent: "center", mt: 4 }}>
        <CircularProgress />
      </Box>
    );
  }

  if (!status) {
    return <Typography>Test run not found</Typography>;
  }

  const latencyChartData = {
    datasets: [
      {
        label: "Average Latency (ms)",
        data: latencyMetrics.map((p) => ({
          x: p.timestamp?.toDate(),
          y: p.value,
        })),
        borderColor: "rgb(75, 192, 192)",
        tension: 0.1,
        pointRadius: 6,
        pointHoverRadius: 10,
      },
    ],
  };

  const rpsChartData = {
    datasets: [
      {
        label: "RPS",
        data: rpsMetrics.map((p) => ({
          x: p.timestamp?.toDate(),
          y: p.value / 5,
        })),
        borderColor: "rgb(54, 162, 235)",
        tension: 0.1,
        pointRadius: 6,
        pointHoverRadius: 10,
      },
    ],
  };

  const commonChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    scales: {
      x: {
        type: "time" as const,
        time: {
          unit: "second" as const,
        },
        title: {
          display: true,
          text: "Time",
        },
      },
      y: {
        beginAtZero: true,
      },
    },
  };

  const latencyChartOptions = {
    ...commonChartOptions,
    scales: {
      ...commonChartOptions.scales,
      x: {
        ...commonChartOptions.scales.x,
        min: startTime?.getTime(),
        max: (status.isRunning
          ? new Date()
          : endTime?.toISOString().startsWith("1970")
            ? new Date()
            : endTime
        )?.getTime(),
        time: {
          ...commonChartOptions.scales.x.time,
          displayFormats: {
            second: "HH:mm:ss",
          },
        },
      },
      y: {
        ...commonChartOptions.scales.y,
        title: {
          display: true,
          text: "Latency (ms)",
        },
      },
    },
  };

  const rpsChartOptions = {
    ...commonChartOptions,
    scales: {
      ...commonChartOptions.scales,
      x: {
        ...commonChartOptions.scales.x,
        min: startTime?.getTime(),
        max: (status.isRunning
          ? new Date()
          : endTime?.toISOString().startsWith("1970")
            ? new Date()
            : endTime
        )?.getTime(),
        time: {
          ...commonChartOptions.scales.x.time,
          displayFormats: {
            second: "HH:mm:ss",
          },
        },
      },
      y: {
        ...commonChartOptions.scales.y,
        title: {
          display: true,
          text: "RPS",
        },
      },
    },
  };

  return (
    <Box>
      <Box
        sx={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          mb: 3,
        }}
      >
        <Typography variant="h4">Test Run Detail</Typography>
        <Box sx={{ display: "flex", gap: 2 }}>
          <Button
            variant="outlined"
            onClick={() => navigate(`/query?testRunId=${id}`)}
          >
            Query Metrics
          </Button>
          <Button
            variant="outlined"
            onClick={handleExport}
            disabled={exporting}
          >
            {exporting ? "Exporting..." : "Export Report"}
          </Button>
          {config.s3Enabled && (
            <Button
              variant="outlined"
              onClick={handleExportToS3}
              disabled={exportingS3}
            >
              {exportingS3 ? "Exporting to S3..." : "Export to S3"}
            </Button>
          )}
          {status.isRunning && (
            <Button
              variant="contained"
              color="error"
              onClick={handleStop}
              disabled={stopping}
            >
              {stopping ? "Stopping..." : "Stop Test"}
            </Button>
          )}
        </Box>
      </Box>

      <Grid container spacing={3}>
        <Grid size={{ xs: 12, md: 3 }}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Status
              </Typography>
              <Typography
                variant="h5"
                color={status.isRunning ? "primary" : "textPrimary"}
              >
                {status.isRunning ? "Running" : "Completed"}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid size={{ xs: 12, md: 3 }}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Start Time
              </Typography>
              <Typography variant="h6">
                {startTime ? startTime.toLocaleString() : "-"}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid size={{ xs: 12, md: 3 }}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                {status.isRunning ? "Expected End Time" : "End Time"}
              </Typography>
              <Typography variant="h6">
                {status.isRunning
                  ? expectedEndTime
                    ? expectedEndTime.toLocaleString()
                    : "-"
                  : endTime && !endTime.toISOString().startsWith("1970")
                    ? endTime.toLocaleString()
                    : "-"}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid size={{ xs: 12, md: 3 }}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                {status.isRunning ? "Remaining Time" : "Duration"}
              </Typography>
              <Typography variant="h5">
                {status.isRunning
                  ? remainingSec !== null
                    ? `${remainingSec}s`
                    : "-"
                  : `${durationSec}s`}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      <Grid container spacing={3} sx={{ mt: 2 }}>
        <Grid size={{ xs: 12, md: 4, lg: 2.4 }}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                RPS
              </Typography>
              <Typography variant="h5">{calculatedRps.toFixed(2)}</Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid size={{ xs: 12, md: 4, lg: 2.4 }}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Success / Failure
              </Typography>
              <Typography variant="h5">
                {status.totalSuccess} / {status.totalFailure}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid size={{ xs: 12, md: 4, lg: 2.4 }}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Latency (Average)
              </Typography>
              <Typography variant="h5">
                {status.avgLatencyMs.toFixed(2)} ms
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid size={{ xs: 12, md: 4, lg: 2.4 }}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Latency (P90)
              </Typography>
              <Typography variant="h5">
                {status.p90LatencyMs.toFixed(2)} ms
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid size={{ xs: 12, md: 4, lg: 2.4 }}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Latency (P95)
              </Typography>
              <Typography variant="h5">
                {status.p95LatencyMs.toFixed(2)} ms
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid size={{ xs: 12, md: 4, lg: 2.4 }}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Concurrency (VUs)
              </Typography>
              <Typography variant="h5">{status.concurrency}</Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid size={{ xs: 12, md: 4, lg: 2.4 }}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Workers
              </Typography>
              <Typography variant="h5">{status.workerCount}</Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      <Grid container spacing={3} sx={{ mt: 2 }}>
        <Grid size={{ xs: 12, lg: 6 }}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Latency Over Time
            </Typography>
            <Box sx={{ height: 400 }}>
              <Line data={latencyChartData} options={latencyChartOptions} />
            </Box>
          </Paper>
        </Grid>
        <Grid size={{ xs: 12, lg: 6 }}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              RPS Over Time
            </Typography>
            <Box sx={{ height: 400 }}>
              <Line data={rpsChartData} options={rpsChartOptions} />
            </Box>
          </Paper>
        </Grid>

        {status.pathMetrics && Object.keys(status.pathMetrics).length > 0 && (
          <Grid size={{ xs: 12 }}>
            <Typography variant="h6" gutterBottom sx={{ mt: 3 }}>
              Path Metrics
            </Typography>
            <TableContainer
              component={Paper}
              sx={{ maxWidth: "100%", overflowX: "auto" }}
            >
              <Table stickyHeader>
                <TableHead>
                  <TableRow>
                    <TableCell>Method</TableCell>
                    <TableCell sx={{ minWidth: "300px" }}>Path</TableCell>
                    <TableCell align="right">Success</TableCell>
                    <TableCell align="right">Failure</TableCell>
                    <TableCell align="right">RPS</TableCell>
                    <TableCell align="right">Avg (ms)</TableCell>
                    <TableCell align="right">Min (ms)</TableCell>
                    <TableCell align="right">Max (ms)</TableCell>
                    <TableCell align="right">P90 (ms)</TableCell>
                    <TableCell align="right">P95 (ms)</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {Object.entries(status.pathMetrics)
                    .filter(([path]) => path !== "scenario_iteration")
                    .map(([path, metrics]) => (
                      <TableRow key={path}>
                        <TableCell>{metrics.method}</TableCell>
                        <TableCell
                          component="th"
                          scope="row"
                          sx={{
                            wordBreak: "break-all",
                            minWidth: "300px",
                            maxWidth: "500px",
                          }}
                        >
                          {path}
                        </TableCell>
                        <TableCell align="right">
                          {metrics.totalSuccess}
                        </TableCell>
                        <TableCell align="right">
                          {metrics.totalFailure}
                        </TableCell>
                        <TableCell align="right">
                          {metrics.rps.toFixed(2)}
                        </TableCell>
                        <TableCell align="right">
                          {metrics.avgLatencyMs.toFixed(2)}
                        </TableCell>
                        <TableCell align="right">
                          {metrics.minLatencyMs.toFixed(2)}
                        </TableCell>
                        <TableCell align="right">
                          {metrics.maxLatencyMs.toFixed(2)}
                        </TableCell>
                        <TableCell align="right">
                          {metrics.p90LatencyMs.toFixed(2)}
                        </TableCell>
                        <TableCell align="right">
                          {metrics.p95LatencyMs.toFixed(2)}
                        </TableCell>
                      </TableRow>
                    ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Grid>
        )}
      </Grid>
    </Box>
  );
};

export default TestRunDetail;
