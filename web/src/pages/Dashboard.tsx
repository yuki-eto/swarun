import {
  Box,
  Button,
  Card,
  CardContent,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Grid,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Typography,
} from "@mui/material";
import { useCallback, useEffect, useState } from "react";
import { FiTrash2 } from "react-icons/fi";
import { useNavigate } from "react-router-dom";
import { client } from "../api/client";
import type { WorkerInfo } from "../gen/swarun_pb";

const Dashboard = () => {
  const [workers, setWorkers] = useState<WorkerInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [openDialog, setOpenDialog] = useState(false);
  const [openProvisionDialog, setOpenProvisionDialog] = useState(false);
  const [provisionCount, setProvisionCount] = useState(1);
  const [provisioning, setProvisioning] = useState(false);
  const [tearingDown, setTearingDown] = useState(false);
  const [tearingDownWorkerId, setTearingDownWorkerId] = useState<string | null>(
    null,
  );
  const [concurrency, setConcurrency] = useState(10);
  const [duration, setDuration] = useState("60s");
  const [rampUp, setRampUp] = useState("0s");
  const [stages, setStages] = useState("");
  const [metadata, setMetadata] = useState("");
  const [exporting, setExporting] = useState(false);
  const [importing, setImporting] = useState(false);
  const navigate = useNavigate();

  const fetchData = useCallback(async () => {
    try {
      const workerResp = await client.listWorkers({});
      setWorkers(workerResp.workers);
    } catch (err) {
      console.error("Failed to fetch data:", err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, [fetchData]);

  const handleProvisionWorkers = async () => {
    try {
      setProvisioning(true);
      const resp = await client.provisionWorkers({
        count: provisionCount,
      });
      if (resp.success) {
        setOpenProvisionDialog(false);
        fetchData();
      } else {
        alert(`Failed to provision workers: ${resp.message}`);
      }
    } catch (err) {
      console.error("Failed to provision workers:", err);
      alert("Error provisioning workers");
    } finally {
      setProvisioning(false);
    }
  };

  const handleTeardownWorkers = async () => {
    if (
      !confirm(
        "Are you sure you want to stop and remove all dynamically provisioned workers?",
      )
    ) {
      return;
    }
    try {
      setTearingDown(true);
      const resp = await client.teardownWorkers({});
      if (resp.success) {
        fetchData();
      } else {
        alert(`Failed to teardown workers: ${resp.message}`);
      }
    } catch (err) {
      console.error("Failed to teardown workers:", err);
      alert("Error tearing down workers");
    } finally {
      setTearingDown(false);
    }
  };

  const handleTeardownWorker = async (workerId: string) => {
    if (
      !confirm(`Are you sure you want to stop and remove worker ${workerId}?`)
    ) {
      return;
    }
    try {
      setTearingDownWorkerId(workerId);
      const resp = await client.teardownWorker({
        workerId: workerId,
      });
      if (resp.success) {
        fetchData();
      } else {
        alert(`Failed to teardown worker: ${resp.message}`);
      }
    } catch (err) {
      console.error("Failed to teardown worker:", err);
      alert("Error tearing down worker");
    } finally {
      setTearingDownWorkerId(null);
    }
  };

  const parseDurationToSeconds = (dur: string): bigint => {
    const match = dur.trim().match(/^(\d+)(s|m|h)?$/);
    if (!match) return BigInt(parseInt(dur, 10) || 60);

    const value = parseInt(match[1], 10);
    const unit = match[2];

    switch (unit) {
      case "m":
        return BigInt(value * 60);
      case "h":
        return BigInt(value * 3600);
      default:
        return BigInt(value);
    }
  };

  const handleStartTest = async () => {
    try {
      const rampingStages = stages
        .split(",")
        .filter((s) => s.trim() !== "")
        .map((s) => {
          const [target, dur] = s.split(":");
          return {
            target: parseInt(target, 10),
            duration: { seconds: parseDurationToSeconds(dur), nanos: 0 },
          };
        });

      const resp = await client.runTest({
        testConfig: {
          concurrency,
          duration: {
            seconds: parseDurationToSeconds(duration),
            nanos: 0,
          },
          rampUpDuration: {
            seconds: parseDurationToSeconds(rampUp),
            nanos: 0,
          },
          stages: rampingStages,
          metadata,
        },
      });
      if (resp.success) {
        setOpenDialog(false);
        fetchData();
        navigate(`/runs/${resp.testRunId}`);
      } else {
        alert(`Failed to start test: ${resp.message}`);
      }
    } catch (err) {
      console.error("Failed to start test:", err);
      alert("Error starting test. Check stages format (e.g. 10:30s,20:1m)");
    }
  };

  const handleExportData = async () => {
    try {
      setExporting(true);
      const chunks: Uint8Array[] = [];
      for await (const res of client.exportData({})) {
        chunks.push(res.chunk);
      }
      const blob = new Blob(chunks as BlobPart[], { type: "application/zip" });
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `swarun-data-${new Date().toISOString().replace(/[:.]/g, "-")}.zip`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
    } catch (err) {
      console.error("Failed to export data:", err);
      alert("Failed to export data");
    } finally {
      setExporting(false);
    }
  };

  const handleImportData = async (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    const file = event.target.files?.[0];
    if (!file) return;

    try {
      setImporting(true);
      const stream = async function* () {
        const reader = file.stream().getReader();
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          yield { chunk: value };
        }
      };

      const resp = await client.importData(stream());
      if (resp.success) {
        alert("Data imported successfully");
        fetchData();
      } else {
        alert(`Failed to import data: ${resp.message}`);
      }
    } catch (err) {
      console.error("Failed to import data:", err);
      alert("Failed to import data");
    } finally {
      setImporting(false);
      event.target.value = "";
    }
  };

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Dashboard
      </Typography>

      <Grid container spacing={3}>
        <Grid size={{ xs: 12, md: 8 }}>
          <Box sx={{ display: "flex", flexDirection: "column", gap: 3 }}>
            <Card>
              <CardContent>
                <Box
                  sx={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    mb: 2,
                  }}
                >
                  <Typography variant="h6">Available Workers</Typography>
                  <Button
                    size="small"
                    variant="outlined"
                    color="error"
                    onClick={handleTeardownWorkers}
                    disabled={tearingDown || workers.length === 0}
                  >
                    {tearingDown ? (
                      <CircularProgress size={20} />
                    ) : (
                      "Teardown All"
                    )}
                  </Button>
                </Box>
                {loading ? (
                  <CircularProgress />
                ) : (
                  <TableContainer component={Paper} variant="outlined">
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell>Worker ID</TableCell>
                          <TableCell>Hostname</TableCell>
                          <TableCell>Address</TableCell>
                          <TableCell>Last Heartbeat</TableCell>
                          <TableCell align="right">Actions</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {workers.map((worker) => (
                          <TableRow key={worker.workerId}>
                            <TableCell>{worker.workerId}</TableCell>
                            <TableCell>{worker.hostname}</TableCell>
                            <TableCell>{worker.address}</TableCell>
                            <TableCell>
                              {worker.lastHeartbeat?.toDate().toLocaleString()}
                            </TableCell>
                            <TableCell align="right">
                              <Button
                                size="small"
                                color="error"
                                onClick={() =>
                                  handleTeardownWorker(worker.workerId)
                                }
                                disabled={
                                  tearingDownWorkerId === worker.workerId
                                }
                              >
                                {tearingDownWorkerId === worker.workerId ? (
                                  <CircularProgress size={20} />
                                ) : (
                                  <FiTrash2 size={18} title="Teardown Worker" />
                                )}
                              </Button>
                            </TableCell>
                          </TableRow>
                        ))}
                        {workers.length === 0 && (
                          <TableRow>
                            <TableCell colSpan={5} align="center">
                              No workers registered
                            </TableCell>
                          </TableRow>
                        )}
                      </TableBody>
                    </Table>
                  </TableContainer>
                )}
              </CardContent>
            </Card>
          </Box>
        </Grid>

        <Grid size={{ xs: 12, md: 4 }}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Actions
              </Typography>
              <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
                <Button
                  variant="contained"
                  color="primary"
                  onClick={() => setOpenDialog(true)}
                  disabled={workers.length === 0}
                >
                  Start New Test
                </Button>
                <Button
                  variant="contained"
                  color="success"
                  onClick={() => setOpenProvisionDialog(true)}
                  disabled={provisioning}
                >
                  {provisioning ? (
                    <CircularProgress size={24} />
                  ) : (
                    "Provision Workers"
                  )}
                </Button>
                <Button
                  variant="outlined"
                  color="secondary"
                  onClick={handleExportData}
                  disabled={exporting}
                >
                  {exporting ? (
                    <CircularProgress size={24} />
                  ) : (
                    "Export Data (ZIP)"
                  )}
                </Button>
                <Button
                  variant="outlined"
                  component="label"
                  disabled={importing}
                >
                  {importing ? (
                    <CircularProgress size={24} />
                  ) : (
                    "Import Data (ZIP)"
                  )}
                  <input
                    type="file"
                    hidden
                    accept=".zip"
                    onChange={handleImportData}
                  />
                </Button>
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      <Dialog
        open={openProvisionDialog}
        onClose={() => setOpenProvisionDialog(false)}
      >
        <DialogTitle>Provision New Workers</DialogTitle>
        <DialogContent>
          <Box sx={{ display: "flex", flexDirection: "column", gap: 2, mt: 1 }}>
            <Typography variant="body2" color="text.secondary">
              Dynamically start new workers using the configured platform (e.g.,
              Docker).
            </Typography>
            <TextField
              label="Number of Workers"
              type="number"
              value={provisionCount}
              onChange={(e) =>
                setProvisionCount(Math.max(1, parseInt(e.target.value, 10)))
              }
              fullWidth
              autoFocus
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpenProvisionDialog(false)}>Cancel</Button>
          <Button
            onClick={handleProvisionWorkers}
            variant="contained"
            color="primary"
            disabled={provisioning}
          >
            {provisioning ? <CircularProgress size={24} /> : "Provision"}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={openDialog} onClose={() => setOpenDialog(false)}>
        <DialogTitle>Start New Test</DialogTitle>
        <DialogContent>
          <Box sx={{ display: "flex", flexDirection: "column", gap: 2, mt: 1 }}>
            <TextField
              label="Concurrency"
              type="number"
              value={concurrency}
              onChange={(e) => setConcurrency(parseInt(e.target.value, 10))}
              fullWidth
              helperText="Base concurrency (if not using stages)"
            />
            <TextField
              label="Duration (e.g. 60s, 5m)"
              value={duration}
              onChange={(e) => setDuration(e.target.value)}
              fullWidth
              helperText="Total test duration"
            />
            <TextField
              label="Ramp-up (e.g. 30s)"
              value={rampUp}
              onChange={(e) => setRampUp(e.target.value)}
              fullWidth
              helperText="Duration to reach target concurrency"
            />
            <TextField
              label="Stages (optional)"
              placeholder="10:30s,20:1m,20:2m"
              value={stages}
              onChange={(e) => setStages(e.target.value)}
              fullWidth
              helperText="Format: target:duration,target:duration"
            />
            <TextField
              label="Metadata (optional)"
              placeholder='{"key": "value"}'
              value={metadata}
              onChange={(e) => setMetadata(e.target.value)}
              fullWidth
              multiline
              rows={3}
              helperText="Metadata for scenario (JSON string, etc.)"
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpenDialog(false)}>Cancel</Button>
          <Button onClick={handleStartTest} variant="contained" color="primary">
            Start
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default Dashboard;
