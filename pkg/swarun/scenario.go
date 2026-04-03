package swarun

import "context"

// Scenario は負荷試験のシナリオを定義するインターフェースです。
type Scenario interface {
	// Run はテストシナリオの1回分の実行を担当します。
	// Runner によって並列に呼び出されます。
	Run(ctx context.Context, metadata string) error
}

// ScenarioFunc は関数を Scenario インターフェースに適合させるためのアダプターです。
type ScenarioFunc func(ctx context.Context, metadata string) error

func (f ScenarioFunc) Run(ctx context.Context, metadata string) error {
	return f(ctx, metadata)
}
