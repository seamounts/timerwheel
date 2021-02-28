package timerwheel

import (
	"container/list"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Task 延时任务
type Task struct {
	expiration int64 // in milliseconds
	b          unsafe.Pointer
	element    *list.Element

	// 任务唯一标志
	key string
}

func NewTask(expiration int64, key string) *Task {
	return &Task{
		expiration: expiration,
		key:        key,
	}
}

func (t *Task) GetKey() string {
	return t.key
}

func (t *Task) getBucket() *bucket {
	return (*bucket)(atomic.LoadPointer(&t.b))
}

func (t *Task) setBucket(b *bucket) {
	atomic.StorePointer(&t.b, unsafe.Pointer(b))
}

func (t *Task) Stop() bool {
	stopped := false
	for b := t.getBucket(); b != nil; b = t.getBucket() {
		// If b.Remove is called just after the timing wheel's goroutine has:
		//     1. removed t from b (through b.Flush -> b.remove)
		//     2. moved t from b to another bucket ab (through b.Flush -> b.remove and ab.Add)
		// this may fail to remove t due to the change of t's bucket.
		stopped = b.Remove(t)

		// Thus, here we re-get t's possibly new bucket (nil for case 1, or ab (non-nil) for case 2),
		// and retry until the bucket becomes nil, which indicates that t has finally been removed.
	}
	return stopped
}

type bucket struct {
	// 64-bit atomic operations require 64-bit alignment, but 32-bit
	// compilers do not ensure it. So we must keep the 64-bit field
	// as the first field of the struct.
	//
	// For more explanations, see https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	// and https://go101.org/article/memory-layout.html.
	expiration int64

	mu     sync.Mutex
	timers *list.List
}

func newBucket() *bucket {
	return &bucket{
		timers:     list.New(),
		expiration: -1,
	}
}

func (b *bucket) Expiration() int64 {
	return atomic.LoadInt64(&b.expiration)
}

func (b *bucket) SetExpiration(expiration int64) bool {
	return atomic.SwapInt64(&b.expiration, expiration) != expiration
}

func (b *bucket) Add(t *Task) {
	b.mu.Lock()

	e := b.timers.PushBack(t)
	t.setBucket(b)
	t.element = e

	b.mu.Unlock()
}

func (b *bucket) remove(t *Task) bool {
	if t.getBucket() != b {
		// If remove is called from t.Stop, and this happens just after the timing wheel's goroutine has:
		//     1. removed t from b (through b.Flush -> b.remove)
		//     2. moved t from b to another bucket ab (through b.Flush -> b.remove and ab.Add)
		// then t.getBucket will return nil for case 1, or ab (non-nil) for case 2.
		// In either case, the returned value does not equal to b.
		return false
	}
	b.timers.Remove(t.element)
	t.setBucket(nil)
	t.element = nil
	return true
}

// Remove 移除延时任务
func (b *bucket) Remove(t *Task) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remove(t)
}

// fushTasks pop all task in bucket
func (b *bucket) flushTasks() []*Task {
	var ts []*Task

	b.mu.Lock()
	for e := b.timers.Front(); e != nil; {
		next := e.Next()

		t := e.Value.(*Task)
		b.remove(t)

		ts = append(ts, t)

		e = next
	}
	b.mu.Unlock()

	b.SetExpiration(-1)

	return ts
}
