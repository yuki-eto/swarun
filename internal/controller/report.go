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
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 1000px; margin: 0 auto; padding: 20px; }
        .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid #eee; padding-bottom: 10px; margin-bottom: 20px; }
        .summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .card { background: #f9f9f9; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
        .card h3 { margin-top: 0; color: #666; font-size: 0.9rem; text-transform: uppercase; }
        .card .value { font-size: 1.5rem; font-weight: bold; }
        .chart-container { height: 400px; margin-bottom: 40px; position: relative; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #eee; }
        th { background-color: #f5f5f5; color: #666; font-size: 0.8rem; text-transform: uppercase; }
        tr:hover { background-color: #fcfcfc; }
        .align-right { text-align: right; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Swarun Test Report</h1>
        <div>ID: {{.Status.TestRunId}}</div>
    </div>

    <div class="summary-grid">
        <div class="card">
            <h3>Duration</h3>
            <div class="value">{{.Elapsed}}</div>
        </div>
        <div class="card">
            <h3>Avg RPS</h3>
            <div class="value">{{printf "%.2f" .Status.Rps}}</div>
        </div>
        <div class="card">
            <h3>Success / Failure</h3>
            <div class="value">{{.Status.TotalSuccess}} / {{.Status.TotalFailure}}</div>
        </div>
        <div class="card">
            <h3>Avg Latency</h3>
            <div class="value">{{printf "%.2f" .Status.AvgLatencyMs}} ms</div>
        </div>
        <div class="card">
            <h3>P95 Latency</h3>
            <div class="value">{{printf "%.2f" .Status.P95LatencyMs}} ms</div>
        </div>
    </div>

    {{if .Status.PathMetrics}}
    <h2>Path Metrics</h2>
    <table>
        <thead>
            <tr>
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
                <td>{{$path}}</td>
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
    {{end}}

    <h2>Latency Over Time</h2>
    <div class="chart-container">
        <canvas id="latencyChart"></canvas>
    </div>

    <h2>RPS Over Time</h2>
    <div class="chart-container">
        <canvas id="rpsChart"></canvas>
    </div>

    <script>
        const latencyMetrics = {{.LatencyMetricsJSON}};
        const rpsMetrics = {{.RPSMetricsJSON}};
        const startTime = new Date({{.Status.StartTime.AsTime.Format "2006-01-02T15:04:05Z07:00"}});
        const endTime = {{if .Status.EndTime.AsTime.IsZero}}new Date(){{else}}new Date({{.Status.EndTime.AsTime.Format "2006-01-02T15:04:05Z07:00"}}){{end}};
        
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
                    tension: 0.1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        type: 'time',
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
                        y: p.value / 5 // 5s aggregate window so divide by 5 for per-second rate
                    })),
                    borderColor: 'rgb(54, 162, 235)',
                    tension: 0.1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        type: 'time',
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

	elapsed := "N/A"
	if !status.StartTime.AsTime().IsZero() {
		end := status.EndTime.AsTime()
		if end.IsZero() {
			end = time.Now()
		}
		elapsed = end.Sub(status.StartTime.AsTime()).Round(time.Second).String()
	}

	var buf bytes.Buffer
	data := struct {
		Status             *swarunv1.GetTestStatusResponse
		LatencyMetricsJSON template.JS
		RPSMetricsJSON     template.JS
		Elapsed            string
	}{
		Status:             status,
		LatencyMetricsJSON: template.JS(latencyJSON),
		RPSMetricsJSON:     template.JS(rpsJSON),
		Elapsed:            elapsed,
	}

	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}

	return buf.String(), nil
}
