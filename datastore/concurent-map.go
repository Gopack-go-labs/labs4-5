package datastore

import "sync"

type ConcurrentMap[K comparable, V any] struct {
	m map[K]V
	l *sync.RWMutex
}

func ConcurrentMapInit[K comparable, V any]() *ConcurrentMap[K, V] {
	return &ConcurrentMap[K, V]{
		m: make(map[K]V),
		l: &sync.RWMutex{},
	}
}

func (cm *ConcurrentMap[K, V]) Set(key K, value V) {
	cm.l.Lock()
	defer cm.l.Unlock()
	cm.m[key] = value
}

func (cm *ConcurrentMap[K, V]) ReplaceOwn(key K, o *ConcurrentMap[K, V]) {
	val, _ := o.Get(key)
	cm.Set(key, val)
}

func (cm *ConcurrentMap[K, V]) Get(key K) (V, bool) {
	cm.l.RLock()
	defer cm.l.RUnlock()
	value, ok := cm.m[key]
	return value, ok
}

func (cm *ConcurrentMap[K, V]) Delete(key K) {
	cm.l.Lock()
	defer cm.l.Unlock()
	delete(cm.m, key)
}
