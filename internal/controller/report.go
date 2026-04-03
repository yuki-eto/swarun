package controller

import (
	"bytes"
	"encoding/json"
	"html/template"
	"time"

	swarunv1 "github.com/yuki-eto/swarun/gen/proto/v1"
)

const reportTemplate = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Swarun Test Report - {{.Status.TestRunId}}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns"></script>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 1200px; margin: 0 auto; padding: 20px; background-color: #f5f5f5; }
        .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; }
        .header h1 { margin: 0; font-size: 2.125rem; font-weight: 400; }
        .summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 24px; margin-bottom: 24px; }
        .card { background: #fff; padding: 16px; border-radius: 4px; box-shadow: 0px 2px 1px -1px rgba(0,0,0,0.2), 0px 1px 1px 0px rgba(0,0,0,0.14), 0px 1px 3px 0px rgba(0,0,0,0.12); }
        .card h3 { margin: 0 0 8px 0; color: rgba(0, 0, 0, 0.6); font-size: 0.875rem; font-weight: 400; }
        .card .value { font-size: 1.5rem; font-weight: 400; }
        .card .value-primary { color: #1976d2; }
        .charts-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(500px, 1fr)); gap: 24px; margin-bottom: 24px; }
        .chart-card { background: #fff; padding: 16px; border-radius: 4px; box-shadow: 0px 2px 1px -1px rgba(0,0,0,0.2), 0px 1px 1px 0px rgba(0,0,0,0.14), 0px 1px 3px 0px rgba(0,0,0,0.12); }
        .chart-card h2 { margin: 0 0 16px 0; font-size: 1.25rem; font-weight: 500; }
        .chart-container { height: 400px; position: relative; }
        .table-card { background: #fff; padding: 16px; border-radius: 4px; box-shadow: 0px 2px 1px -1px rgba(0,0,0,0.2), 0px 1px 1px 0px rgba(0,0,0,0.14), 0px 1px 3px 0px rgba(0,0,0,0.12); margin-top: 24px; overflow-x: auto; }
        .table-card h2 { margin: 0 0 16px 0; font-size: 1.25rem; font-weight: 500; }
        table { width: 100%; border-collapse: collapse; min-width: 800px; }
        th, td { padding: 12px 16px; text-align: left; border-bottom: 1px solid rgba(224, 224, 224, 1); }
        th { color: rgba(0, 0, 0, 0.87); font-weight: 500; font-size: 0.875rem; background-color: #fff; position: sticky; top: 0; }
        td { color: rgba(0, 0, 0, 0.87); font-size: 0.875rem; }
        tr:hover { background-color: rgba(0, 0, 0, 0.04); }
        .align-right { text-align: right; }
        .path-cell { word-break: break-all; min-width: 300px; max-width: 500px; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Test Run Detail</h1>
        <div style="color: rgba(0,0,0,0.6)">ID: {{.Status.TestRunId}}</div>
    </div>

    <div class="summary-grid">
        <div class="card">
            <h3>Start Time</h3>
            <div class="value" style="font-size: 1.25rem">{{.StartTimeFormatted}}</div>
        </div>
        <div class="card">
            <h3>End Time</h3>
            <div class="value" style="font-size: 1.25rem">{{.EndTimeFormatted}}</div>
        </div>
        <div class="card">
            <h3>Duration</h3>
            <div class="value">{{.Elapsed}}</div>
        </div>
        <div class="card">
            <h3>RPS</h3>
            <div class="value">{{printf "%.2f" .CalculatedRPS}}</div>
        </div>
        <div class="card">
            <h3>Success / Failure</h3>
            <div class="value">{{.Status.TotalSuccess}} / {{.Status.TotalFailure}}</div>
        </div>
        <div class="card">
            <h3>Latency (Average)</h3>
            <div class="value">{{printf "%.2f" .Status.AvgLatencyMs}} ms</div>
        </div>
        <div class="card">
            <h3>Latency (P90)</h3>
            <div class="value">{{printf "%.2f" .Status.P90LatencyMs}} ms</div>
        </div>
        <div class="card">
            <h3>Latency (P95)</h3>
            <div class="value">{{printf "%.2f" .Status.P95LatencyMs}} ms</div>
        </div>
        <div class="card">
            <h3>Concurrency (VUs)</h3>
            <div class="value">{{.Status.Concurrency}}</div>
        </div>
        <div class="card">
            <h3>Workers</h3>
            <div class="value">{{.Status.WorkerCount}}</div>
        </div>
    </div>

    <div class="charts-grid">
        <div class="chart-card">
            <h2>Latency Over Time</h2>
            <div class="chart-container">
                <canvas id="latencyChart"></canvas>
            </div>
        </div>
        <div class="chart-card">
            <h2>RPS Over Time</h2>
            <div class="chart-container">
                <canvas id="rpsChart"></canvas>
            </div>
        </div>
    </div>

    {{if .Status.PathMetrics}}
    <div class="table-card">
        <h2>Path Metrics</h2>
        <table>
            <thead>
                <tr>
                    <th>Method</th>
                    <th>Path</th>
                    <th class="align-right">Success</th>
                    <th class="align-right">Failure</th>
                    <th class="align-right">RPS</th>
                    <th class="align-right">Avg (ms)</th>
                    <th class="align-right">Min (ms)</th>
                    <th class="align-right">Max (ms)</th>
                    <th class="align-right">P90 (ms)</th>
                    <th class="align-right">P95 (ms)</th>
                </tr>
            </thead>
            <tbody>
                {{range $path, $m := .Status.PathMetrics}}
                {{if ne $path "scenario_iteration"}}
                <tr>
                    <td>{{$m.Method}}</td>
                    <td class="path-cell">{{$path}}</td>
                    <td class="align-right">{{$m.TotalSuccess}}</td>
                    <td class="align-right">{{$m.TotalFailure}}</td>
                    <td class="align-right">{{printf "%.2f" $m.Rps}}</td>
                    <td class="align-right">{{printf "%.2f" $m.AvgLatencyMs}}</td>
                    <td class="align-right">{{printf "%.2f" $m.MinLatencyMs}}</td>
                    <td class="align-right">{{printf "%.2f" $m.MaxLatencyMs}}</td>
                    <td class="align-right">{{printf "%.2f" $m.P90LatencyMs}}</td>
                    <td class="align-right">{{printf "%.2f" $m.P95LatencyMs}}</td>
                </tr>
                {{end}}
                {{end}}
            </tbody>
        </table>
    </div>
    {{end}}

    <script>
        const latencyMetrics = {{.LatencyMetricsJSON}};
        const rpsMetrics = {{.RPSMetricsJSON}};
        const startTime = new Date({{.Status.StartTime.AsTime.Format "2006-01-02T15:04:05.999Z07:00"}});
        const endTimeStr = "{{if or .Status.EndTime.AsTime.IsZero ( .Status.EndTime.AsTime.Before .Status.StartTime.AsTime)}}now{{else}}{{.Status.EndTime.AsTime.Format "2006-01-02T15:04:05.999Z07:00"}}{{end}}";
        const endTime = endTimeStr === "now" ? new Date() : new Date(endTimeStr);
        
        const filterPoints = (points) => {
            return points.filter(p => {
                const d = new Date(p.timestamp);
                return d >= startTime && d <= endTime;
            });
        };

        const filteredLatencyMetrics = filterPoints(latencyMetrics);
        const filteredRpsMetrics = filterPoints(rpsMetrics);

        const latencyCtx = document.getElementById('latencyChart').getContext('2d');
        new Chart(latencyCtx, {
            type: 'line',
            data: {
                datasets: [{
                    label: 'Average Latency (ms)',
                    data: filteredLatencyMetrics.map(p => ({
                        x: new Date(p.timestamp),
                        y: p.value
                    })),
                    borderColor: 'rgb(75, 192, 192)',
                    tension: 0.1,
                    pointRadius: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        type: 'time',
                        min: startTime.getTime(),
                        max: endTime.getTime(),
                        time: { 
                            unit: 'second',
                            displayFormats: {
                                second: 'HH:mm:ss'
                            }
                        },
                        title: { display: true, text: 'Time' }
                    },
                    y: {
                        beginAtZero: true,
                        title: { display: true, text: 'Latency (ms)' }
                    }
                }
            }
        });

        const rpsCtx = document.getElementById('rpsChart').getContext('2d');
        new Chart(rpsCtx, {
            type: 'line',
            data: {
                datasets: [{
                    label: 'RPS',
                    data: filteredRpsMetrics.map(p => ({
                        x: new Date(p.timestamp),
                        y: p.value / 5 
                    })),
                    borderColor: 'rgb(54, 162, 235)',
                    tension: 0.1,
                    pointRadius: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        type: 'time',
                        min: startTime.getTime(),
                        max: endTime.getTime(),
                        time: { 
                            unit: 'second',
                            displayFormats: {
                                second: 'HH:mm:ss'
                            }
                        },
                        title: { display: true, text: 'Time' }
                    },
                    y: {
                        beginAtZero: true,
                        title: { display: true, text: 'RPS' }
                    }
                }
            }
        });
    </script>
</body>
</html>
`

func (c *Controller) generateHTMLReport(status *swarunv1.GetTestStatusResponse, latencyPoints []*swarunv1.MetricData, rpsPoints []*swarunv1.MetricData) (string, error) {
	tmpl, err := template.New("report").Parse(reportTemplate)
	if err != nil {
		return "", err
	}

	type displayPoint struct {
		Timestamp time.Time `json:"timestamp"`
		Value     float64   `json:"value"`
	}

	toDisplayPoints := func(points []*swarunv1.MetricData) []displayPoint {
		dps := make([]displayPoint, 0, len(points))
		for _, p := range points {
			dps = append(dps, displayPoint{
				Timestamp: p.Timestamp.AsTime(),
				Value:     p.Value,
			})
		}
		return dps
	}

	latencyJSON, _ := json.Marshal(toDisplayPoints(latencyPoints))
	rpsJSON, _ := json.Marshal(toDisplayPoints(rpsPoints))

	startTime := status.StartTime.AsTime()
	endTime := status.EndTime.AsTime()
	durationSec := float64(0)
	if status.Duration != nil {
		durationSec = float64(status.Duration.Seconds)
	}

	actualDurationSec := float64(1)
	if status.LastRequestTime != nil && status.FirstRequestTime != nil {
		first := status.FirstRequestTime.AsTime()
		last := status.LastRequestTime.AsTime()
		if last.After(first) {
			actualDurationSec = last.Sub(first).Seconds()
		}
	}

	if actualDurationSec < 1 && len(rpsPoints) > 0 {
		first := rpsPoints[0].Timestamp.AsTime()
		last := rpsPoints[len(rpsPoints)-1].Timestamp.AsTime()
		if last.After(first) {
			actualDurationSec = last.Sub(first).Seconds()
		}
	}

	if actualDurationSec < 1 && !startTime.IsZero() {
		if status.IsRunning || endTime.IsZero() || endTime.Unix() < 1000 {
			actualDurationSec = time.Since(startTime).Seconds()
		} else {
			actualDurationSec = endTime.Sub(startTime).Seconds()
		}
	}
	if actualDurationSec < 1 {
		actualDurationSec = max(1, durationSec)
	}

	calculatedRPS := float64(status.TotalSuccess+status.TotalFailure) / actualDurationSec

	elapsed := "N/A"
	if !startTime.IsZero() {
		if status.IsRunning || endTime.IsZero() || endTime.Unix() < 1000 {
			elapsed = time.Since(startTime).Round(time.Second).String()
		} else {
			elapsed = endTime.Sub(startTime).Round(time.Second).String()
		}
	}

	tz := c.cfg.Timezone
	loc, err := time.LoadLocation(tz)
	if err != nil {
		c.logger.Warn("Failed to load timezone, fallback to Local", "timezone", tz, "error", err)
		loc = time.Local
	}

	startTimeFormatted := "-"
	if !startTime.IsZero() {
		startTimeFormatted = startTime.In(loc).Format("2006/1/2 15:04:05")
	}

	endTimeFormatted := "-"
	if !endTime.IsZero() && endTime.Unix() >= 1000 {
		endTimeFormatted = endTime.In(loc).Format("2006/1/2 15:04:05")
	}

	var buf bytes.Buffer
	data := struct {
		Status             *swarunv1.GetTestStatusResponse
		LatencyMetricsJSON template.JS
		RPSMetricsJSON     template.JS
		Elapsed            string
		CalculatedRPS      float64
		StartTimeFormatted string
		EndTimeFormatted   string
	}{
		Status:             status,
		LatencyMetricsJSON: template.JS(latencyJSON),
		RPSMetricsJSON:     template.JS(rpsJSON),
		Elapsed:            elapsed,
		CalculatedRPS:      calculatedRPS,
		StartTimeFormatted: startTimeFormatted,
		EndTimeFormatted:   endTimeFormatted,
	}

	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}

	return buf.String(), nil
}
