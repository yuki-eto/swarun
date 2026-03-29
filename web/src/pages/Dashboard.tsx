import { useEffect, useState } from 'react'
import {
  Typography,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Button,
  Box,
  Card,
  CardContent,
  Grid,
  CircularProgress,
  List,
  ListItem,
  ListItemButton,
  ListItemText,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
} from '@mui/material'
import { useNavigate } from 'react-router-dom'
import { client } from '../api/client'
import { WorkerInfo } from '../gen/swarun_pb'

const Dashboard = () => {
  const [workers, setWorkers] = useState<WorkerInfo[]>([])
  const [testRuns, setTestRuns] = useState<string[]>([])
  const [loading, setLoading] = useState(true)
  const [openDialog, setOpenDialog] = useState(false)
  const [concurrency, setConcurrency] = useState(10)
  const [duration, setDuration] = useState('60s')
  const [stages, setStages] = useState('')
  const [exporting, setExporting] = useState(false)
  const [importing, setImporting] = useState(false)
  const navigate = useNavigate()

  const fetchData = async () => {
    try {
      const [workerResp, testRunResp] = await Promise.all([
        client.listWorkers({}),
        client.listTestRuns({}),
      ])
      setWorkers(workerResp.workers)
      setTestRuns(testRunResp.testRunIds)
    } catch (err) {
      console.error('Failed to fetch data:', err)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchData()
    const interval = setInterval(fetchData, 5000)
    return () => clearInterval(interval)
  }, [])

  const handleStartTest = async () => {
    try {
      const rampingStages = stages.split(',').filter(s => s.trim() !== '').map(s => {
        const [target, dur] = s.split(':')
        let seconds = 0n
        if (dur.endsWith('s')) {
          seconds = BigInt(parseInt(dur))
        } else if (dur.endsWith('m')) {
          seconds = BigInt(parseInt(dur) * 60)
        }
        return {
          target: parseInt(target),
          duration: { seconds, nanos: 0 }
        }
      })

      const resp = await client.runTest({
        testConfig: {
          concurrency,
          duration: {
            seconds: BigInt(parseInt(duration) || 60),
            nanos: 0,
          },
          stages: rampingStages,
        },
      })
      if (resp.success) {
        setOpenDialog(false)
        fetchData()
        navigate(`/runs/${resp.testRunId}`)
      } else {
        alert('Failed to start test: ' + resp.message)
      }
    } catch (err) {
      console.error('Failed to start test:', err)
      alert('Error starting test. Check stages format (e.g. 10:30s,20:1m)')
    }
  }

  const handleExportData = async () => {
    try {
      setExporting(true)
      const chunks: Uint8Array[] = []
      for await (const res of client.exportData({})) {
        chunks.push(res.chunk)
      }
      const blob = new Blob(chunks as any, { type: 'application/zip' })
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `swarun-data-${new Date().toISOString().replace(/[:.]/g, '-')}.zip`
      document.body.appendChild(a)
      a.click()
      window.URL.revokeObjectURL(url)
      document.body.removeChild(a)
    } catch (err) {
      console.error('Failed to export data:', err)
      alert('Failed to export data')
    } finally {
      setExporting(false)
    }
  }

  const handleImportData = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file) return

    try {
      setImporting(true)
      const stream = async function* () {
        const reader = file.stream().getReader()
        while (true) {
          const { done, value } = await reader.read()
          if (done) break
          yield { chunk: value }
        }
      }

      const resp = await client.importData(stream())
      if (resp.success) {
        alert('Data imported successfully')
        fetchData()
      } else {
        alert('Failed to import data: ' + resp.message)
      }
    } catch (err) {
      console.error('Failed to import data:', err)
      alert('Failed to import data')
    } finally {
      setImporting(false)
      event.target.value = ''
    }
  }

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Dashboard
      </Typography>

      <Grid container spacing={3}>
        <Grid size={{ xs: 12, md: 8 }}>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Available Workers
                </Typography>
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
                          </TableRow>
                        ))}
                        {workers.length === 0 && (
                          <TableRow>
                            <TableCell colSpan={4} align="center">
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

            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Recent Test Runs
                </Typography>
                <Paper variant="outlined">
                  <List>
                    {testRuns.map((id) => (
                      <ListItem key={id} disablePadding>
                        <ListItemButton onClick={() => navigate(`/runs/${id}`)}>
                          <ListItemText primary={id} />
                        </ListItemButton>
                      </ListItem>
                    ))}
                    {testRuns.length === 0 && (
                      <ListItem>
                        <ListItemText primary="No test runs found" />
                      </ListItem>
                    )}
                  </List>
                </Paper>
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
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                <Button
                  variant="contained"
                  color="primary"
                  onClick={() => setOpenDialog(true)}
                  disabled={workers.length === 0}
                >
                  Start New Test
                </Button>
                <Button
                  variant="outlined"
                  color="secondary"
                  onClick={handleExportData}
                  disabled={exporting}
                >
                  {exporting ? <CircularProgress size={24} /> : 'Export Data (ZIP)'}
                </Button>
                <Button
                  variant="outlined"
                  component="label"
                  disabled={importing}
                >
                  {importing ? <CircularProgress size={24} /> : 'Import Data (ZIP)'}
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

      <Dialog open={openDialog} onClose={() => setOpenDialog(false)}>
        <DialogTitle>Start New Test</DialogTitle>
        <DialogContent>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mt: 1 }}>
            <TextField
              label="Concurrency"
              type="number"
              value={concurrency}
              onChange={(e) => setConcurrency(parseInt(e.target.value))}
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
              label="Stages (optional)"
              placeholder="10:30s,20:1m,20:2m"
              value={stages}
              onChange={(e) => setStages(e.target.value)}
              fullWidth
              helperText="Format: target:duration,target:duration"
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
  )
}

export default Dashboard
