package atomicmap

import (
	"maps"
	"sync"
)

// Map はジェネリクスを用いたアトミックなマップの実装です。
// sync.RWMutex を使用してスレッドセーフな操作を保証します。
type Map[K comparable, V any] struct {
	m  map[K]V
	mu sync.RWMutex
}

// New は新しい Map を作成し、空のマップで初期化します。
func New[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{
		m: make(map[K]V),
	}
}

// Get は指定されたキーに対応する値を返します。
func (m *Map[K, V]) Get(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.m[key]
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
	m.m = make(map[K]V)
}

// Load は現在のマップのコピーを返します。
func (m *Map[K, V]) Load() map[K]V {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return maps.Clone(m.m)
}

// Swap は現在のマップを新しいマップと入れ替え、古いマップを返します。
func (m *Map[K, V]) Swap(newMap map[K]V) map[K]V {
	m.mu.Lock()
	defer m.mu.Unlock()
	old := m.m
	m.m = newMap
	return old
}

// GetOrCompute は指定されたキーが存在すればその値を返し、存在しなければ compute 関数を実行して
// その結果をマップに追加してから返します。この操作はスレッドセーフです。
// 同一キーに対して compute が同時に一度しか実行されないことを保証するためにミューテックスを使用します。
func (m *Map[K, V]) GetOrCompute(key K, compute func() V) V {
	// 二重チェックロック (Double-checked locking)
	m.mu.RLock()
	if val, ok := m.m[key]; ok {
		m.mu.RUnlock()
		return val
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// ロック取得後に再度チェック
	if val, ok := m.m[key]; ok {
		return val
	}

	val := compute()
	m.m[key] = val

	return val
}

// Set は指定されたキーと値をマップに設定します（既存の値を上書きします）。
func (m *Map[K, V]) Set(key K, val V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.m[key] = val
}

// Delete は指定されたキーとその値をマップから削除します。
func (m *Map[K, V]) Delete(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.m, key)
}
