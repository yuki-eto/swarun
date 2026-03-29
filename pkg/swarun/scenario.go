package swarun

import "context"

// Scenario は負荷試験のシナリオを定義するインターフェースです。
type Scenario interface {
	// Run はテストシナリオの1回分の実行を担当します。
	// Runner によって並列に呼び出されます。
	Run(ctx context.Context) error
}

// ScenarioFunc は関数を Scenario インターフェースに適合させるためのアダプターです。
type ScenarioFunc func(ctx context.Context) error

func (f ScenarioFunc) Run(ctx context.Context) error {
	return f(ctx)
}
