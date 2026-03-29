package controller

import "time"

type Worker struct {
	ID            string
	Hostname      string
	Address       string // ワーカーへの通信に使用するアドレス
	LastHeartbeat time.Time
}
