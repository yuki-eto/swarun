package atomicmap

import (
	"maps"
	"sync"
	"sync/atomic"
)

// Map はジェネリクスを用いたアトミックなマップの実装です。
// 読み取りは完全にロックフリーで、書き込み（新しいキーの追加）には CAS (Compare-And-Swap) ループを使用します。
type Map[K comparable, V any] struct {
	ptr atomic.Pointer[map[K]V]
	mu  sync.Mutex
}

// New は新しい Map を作成し、空のマップで初期化します。
func New[K comparable, V any]() *Map[K, V] {
	m := &Map[K, V]{}
	empty := make(map[K]V)
	m.ptr.Store(&empty)
	return m
}

// Get は指定されたキーに対応する値を返します。
func (m *Map[K, V]) Get(key K) (V, bool) {
	ptr := m.ptr.Load()
	if ptr == nil {
		var zero V
		return zero, false
	}
	val, ok := (*ptr)[key]
	return val, ok
}

// Store は指定されたキーと値をマップに設定します（既存の値を上書きします）。
func (m *Map[K, V]) Store(key K, val V) {
	m.Set(key, val)
}

// Clear はマップを空にします。
func (m *Map[K, V]) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	empty := make(map[K]V)
	m.ptr.Store(&empty)
}

// Load は現在のマップのコピー（ポインタ）を返します。
func (m *Map[K, V]) Load() map[K]V {
	p := m.ptr.Load()
	if p == nil {
		return nil
	}
	return *p
}

// Swap は現在のマップを新しいマップと入れ替え、古いマップを返します。
func (m *Map[K, V]) Swap(newMap map[K]V) map[K]V {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.ptr.Swap(&newMap)
	if p == nil {
		return nil
	}
	return *p
}

// GetOrCompute は指定されたキーが存在すればその値を返し、存在しなければ compute 関数を実行して
// その結果をマップに追加してから返します。この操作はスレッドセーフです。
// 同一キーに対して compute が同時に一度しか実行されないことを保証するためにミューテックスを使用します。
func (m *Map[K, V]) GetOrCompute(key K, compute func() V) V {
	// 読み取りはロックフリー
	oldMapPtr := m.ptr.Load()
	if oldMapPtr != nil {
		if val, ok := (*oldMapPtr)[key]; ok {
			return val
		}
	}

	// 書き込みの競合を避けるために CAS ループを使用するが、
	// compute() 自体が重い場合や副作用がある（DB接続など）場合は、
	// 複数のゴルーチンが同時に compute() を実行しないように制御する必要がある。
	// ここでは atomicmap 全体でロックを持つのではなく、CAS ループ内で
	// 再度チェックすることで、compute() の実行回数を抑える。
	// ただし、CAS に失敗した場合はリトライされるため、依然として compute() が
	// 複数回呼ばれる可能性がある。
	// 厳密に一度だけにしたい場合は、sync.OnceValues や別のロック機構が必要。
	// swarun の用途（コントローラーのストレージ初期化）では、
	// DuckDB の WAL replay エラーを避けるために compute() の並列実行を防ぐ必要がある。

	// 注意: Map 構造体に mutex を追加すると Map のコピー時に問題が出る可能性があるため、
	// 呼び出し側で制御するか、あるいは Map 自体に mutex を持たせる。
	// ここでは実装をシンプルにするため、Map に mutex を追加する。
	m.mu.Lock()
	defer m.mu.Unlock()

	// ロック取得後に再度チェック
	oldMapPtr = m.ptr.Load()
	if oldMapPtr != nil {
		if val, ok := (*oldMapPtr)[key]; ok {
			return val
		}
	}

	val := compute()

	// 新しいマップを作成
	var newMap map[K]V
	if oldMapPtr == nil {
		newMap = make(map[K]V)
	} else {
		newMap = maps.Clone(*oldMapPtr)
	}
	newMap[key] = val
	m.ptr.Store(&newMap)

	return val
}

// Set は指定されたキーと値をマップに設定します（既存の値を上書きします）。
func (m *Map[K, V]) Set(key K, val V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	oldMapPtr := m.ptr.Load()
	var newMap map[K]V
	if oldMapPtr == nil {
		newMap = make(map[K]V)
	} else {
		newMap = maps.Clone(*oldMapPtr)
	}
	newMap[key] = val
	m.ptr.Store(&newMap)
}
