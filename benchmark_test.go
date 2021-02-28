package timerwheel_test

import (
	"testing"
	"time"

	timingwheel "github.com/seamounts/timerwheel"
)

func genD(i int) time.Duration {
	return time.Duration(i%10000) * time.Millisecond
}

func BenchmarkTimingWheel_StartStop(b *testing.B) {
	tw := timingwheel.NewTimerWheel(1*time.Second, 60, execTasks, false)
	tw.Start()
	defer tw.Stop()

	cases := []struct {
		name string
		N    int // the data size (i.e. number of existing timers)
	}{
		{"N-1m", 1000000},
		{"N-5m", 5000000},
		{"N-30m", 30000000},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			base := make([]*timingwheel.Task, c.N)
			for i := 0; i < len(base); i++ {
				base[i] = tw.AddTask(genD(i), "")
			}
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				tw.AddTask(time.Second, "").Stop()
			}

			b.StopTimer()
			for i := 0; i < len(base); i++ {
				base[i].Stop()
			}
		})
	}
}

func BenchmarkStandardTimer_StartStop(b *testing.B) {
	cases := []struct {
		name string
		N    int // the data size (i.e. number of existing timers)
	}{
		{"N-1m", 1000000},
		{"N-5m", 5000000},
		{"N-30m", 30000000},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			base := make([]*time.Timer, c.N)
			for i := 0; i < len(base); i++ {
				base[i] = time.AfterFunc(genD(i), func() {})
			}
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				time.AfterFunc(time.Second, func() {}).Stop()
			}

			b.StopTimer()
			for i := 0; i < len(base); i++ {
				base[i].Stop()
			}
		})
	}
}

func execTasks(ts []*timingwheel.Task) {}
