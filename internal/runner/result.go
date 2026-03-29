package runner

import "time"

type Result struct {
	Success bool
	Latency time.Duration
}
