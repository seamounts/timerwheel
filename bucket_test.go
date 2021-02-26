package timerwheel

import "testing"

func TestBucket_FlushTasks(t *testing.T) {
	b := newBucket()

	b.Add(&Task{})
	b.Add(&Task{})
	l1 := b.timers.Len()
	if l1 != 2 {
		t.Fatalf("Got (%+v) != Want (%+v)", l1, 2)
	}

	b.FlushTasks()
	l2 := b.timers.Len()
	if l2 != 0 {
		t.Fatalf("Got (%+v) != Want (%+v)", l2, 0)
	}
}
