package atomicmap

import (
	"maps"
	"sync/atomic"
)

// Map はジェネリクスを用いたアトミックなマップの実装です。
// 読み取りは完全にロックフリーで、書き込み（新しいキーの追加）には CAS (Compare-And-Swap) ループを使用します。
type Map[K comparable, V any] struct {
	ptr atomic.Pointer[map[K]V]
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
	p := m.ptr.Swap(&newMap)
	if p == nil {
		return nil
	}
	return *p
}

// GetOrCompute は指定されたキーが存在すればその値を返し、存在しなければ compute 関数を実行して
// その結果をマップに追加してから返します。この操作はスレッドセーフです。
func (m *Map[K, V]) GetOrCompute(key K, compute func() V) V {
	// 読み取りはロックフリー
	oldMapPtr := m.ptr.Load()
	if oldMapPtr != nil {
		if val, ok := (*oldMapPtr)[key]; ok {
			return val
		}
	}

	// 新しいラベルの追加には CAS を使用
	for {
		oldMapPtr = m.ptr.Load()
		if oldMapPtr != nil {
			if val, ok := (*oldMapPtr)[key]; ok {
				return val
			}
		}

		// マップをコピーして新しいエントリを追加
		var newMap map[K]V
		if oldMapPtr == nil {
			newMap = make(map[K]V)
		} else {
			newMap = maps.Clone(*oldMapPtr)
		}
		val := compute()
		newMap[key] = val

		if m.ptr.CompareAndSwap(oldMapPtr, &newMap) {
			return val
		}
		// 他のゴルーチンが先に更新した場合はリトライ
	}
}

// Set は指定されたキーと値をマップに設定します（既存の値を上書きします）。
func (m *Map[K, V]) Set(key K, val V) {
	for {
		oldMapPtr := m.ptr.Load()
		newMap := maps.Clone(*oldMapPtr)
		newMap[key] = val

		if m.ptr.CompareAndSwap(oldMapPtr, &newMap) {
			return
		}
	}
}
