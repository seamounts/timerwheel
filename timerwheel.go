package timerwheel

import (
	"errors"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/seamounts/timerwheel/delayqueue"
)

// TimerWheel 实现层级时间轮
type TimerWheel struct {
	tick      int64 // in milliseconds
	wheelSize int64

	interval    int64 // in milliseconds
	currentTime int64 // in milliseconds
	buckets     []*bucket
	queue       *delayqueue.DelayQueue

	// 高层时间轮
	//
	// NOTE: This field may be updated and read concurrently, through Add().
	overflowWheel unsafe.Pointer // type: *TimingWheel

	exitC        chan struct{}
	waitGroup    waitGroupWrapper
	callback     func(ts []*Task)
	callbackSync bool
	runningTasks []*Task
}

// NewTimerWheel creates an instance of TimingWheel with the given tick and wheelSize.
func NewTimerWheel(tick time.Duration, wheelSize int64, callback func(ts []*Task),
	callbackSync bool) *TimerWheel {
	tickMs := int64(tick / time.Millisecond)
	if tickMs <= 0 {
		panic(errors.New("tick must be greater than or equal to 1ms"))
	}

	startMs := timeToMs(time.Now().UTC())

	return newTimerWheel(
		tickMs,
		wheelSize,
		startMs,
		delayqueue.New(int(wheelSize)),
		callback,
		callbackSync,
	)
}

// newTimerWheel is an internal helper function that really creates an instance of TimingWheel.
func newTimerWheel(tickMs int64, wheelSize int64, startMs int64, queue *delayqueue.DelayQueue,
	callback func(ts []*Task), callbackSync bool) *TimerWheel {
	buckets := make([]*bucket, wheelSize)
	for i := range buckets {
		buckets[i] = newBucket()
	}

	tw := &TimerWheel{
		tick:         tickMs,
		wheelSize:    wheelSize,
		currentTime:  truncate(startMs, tickMs),
		interval:     tickMs * wheelSize,
		buckets:      buckets,
		queue:        queue,
		callback:     callback,
		callbackSync: callbackSync,
		exitC:        make(chan struct{}),
	}

	return tw
}

// add 将延时任务加入时间轮
func (tw *TimerWheel) add(t *Task) {
	currentTime := atomic.LoadInt64(&tw.currentTime)

	// 将task插入第一层时间轮
	if t.expiration < currentTime+tw.interval {
		virtualID := t.expiration / tw.tick
		b := tw.buckets[virtualID%tw.wheelSize]
		b.Add(t)

		// 设置 bucket 的过期时间
		if b.SetExpiration(virtualID * tw.tick) {
			// The bucket needs to be enqueued since it was an expired bucket.
			// We only need to enqueue the bucket when its expiration time has changed,
			// i.e. the wheel has advanced and this bucket get reused with a new expiration.
			// Any further calls to set the expiration within the same wheel cycle will
			// pass in the same value and hence return false, thus the bucket with the
			// same expiration will not be enqueued multiple times.
			tw.queue.Offer(b, b.Expiration())
		}

	} else {
		// 将任务放置到外层时间轮
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel == nil {

			atomic.CompareAndSwapPointer(
				&tw.overflowWheel,
				nil,
				unsafe.Pointer(newTimerWheel(
					tw.interval,
					tw.wheelSize,
					currentTime,
					tw.queue,
					tw.callback,
					tw.callbackSync,
				)),
			)
			overflowWheel = atomic.LoadPointer(&tw.overflowWheel)
		}
		(*TimerWheel)(overflowWheel).add(t)
	}
}

// advanceClock 驱动时间轮的指针移动
func (tw *TimerWheel) advanceClock(expiration int64) {
	currentTime := atomic.LoadInt64(&tw.currentTime)
	if expiration >= currentTime+tw.tick {
		currentTime = truncate(expiration, tw.tick)
		atomic.StoreInt64(&tw.currentTime, currentTime)

		// Try to advance the clock of the overflow wheel if present
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel != nil {
			(*TimerWheel)(overflowWheel).advanceClock(currentTime)
		}
	}
}

// reInsert 将任务重新插入时间轮
// 通常是将高层时间轮的任务降级插入到低级时间轮
func (tw *TimerWheel) reInsert(ts []*Task) {
	for _, t := range ts {
		// 将已经到期的任务放入到 runningTasks 列表
		if t.expiration < tw.currentTime+tw.tick {
			tw.runningTasks = append(tw.runningTasks, t)
		} else {
			tw.add(t)
		}
	}

	if len(tw.runningTasks) > 0 {
		// 唤醒 runningTasksWakeUP ，执行到期任务
		tw.runTasks()
	}
}

// runTasks 运行到期任务，任务异步执行
func (tw *TimerWheel) runTasks() {
	if len(tw.runningTasks) == 0 {
		return
	}
	if tw.callback == nil {
		return
	}

	if tw.callbackSync {
		tw.callback(tw.runningTasks)
	} else {
		go tw.callback(tw.runningTasks)
	}

	tw.runningTasks = nil
}

// Start starts the current timing wheel.
func (tw *TimerWheel) Start() {
	tw.waitGroup.Wrap(func() {
		tw.queue.Poll(tw.exitC, func() int64 {
			return timeToMs(time.Now().UTC())
		})
	})

	tw.waitGroup.Wrap(func() {
		for {
			select {
			case elem := <-tw.queue.C:
				b := elem.(*bucket)
				tw.advanceClock(b.Expiration())

				ts := b.FlushTasks()

				// 任务重新插入时间轮
				tw.reInsert(ts)
			case <-tw.exitC:
				return
			}
		}
	})

	tw.waitGroup.Wrap(func() {
		tw.runTasks()
	})
}

// Stop stops the current timing wheel.
//
// If there is any timer's task being running in its own goroutine, Stop does
// not wait for the task to complete before returning. If the caller needs to
// know whether the task is completed, it must coordinate with the task explicitly.
func (tw *TimerWheel) Stop() {
	close(tw.exitC)
	tw.waitGroup.Wait()
}

// AddTask 加入定时任务
func (tw *TimerWheel) AddTask(d time.Duration, key string) *Task {
	t := &Task{
		expiration: timeToMs(time.Now().UTC().Add(d)),
		key:        key,
	}
	tw.add(t)

	return t
}
