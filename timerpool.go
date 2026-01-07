package remotewrite

import (
	"sync"
	"time"
)

// timerPool is a pool of reusable timers to reduce GC pressure.
// Copied from: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/lib/timerpool/timerpool.go
//
// Usage:
//
//	t := getTimer(duration)
//	select {
//	case <-t.C:
//	    // timer fired
//	case <-stopCh:
//	    // cancelled
//	}
//	putTimer(t)
var timerPool sync.Pool

// getTimer returns a timer for the given duration from the pool.
// The timer must be returned to the pool with putTimer after use.
func getTimer(d time.Duration) *time.Timer {
	if v := timerPool.Get(); v != nil {
		t := v.(*time.Timer)
		if t.Reset(d) {
			// Timer was still active - this shouldn't happen if used correctly.
			// Drain the channel to be safe.
			select {
			case <-t.C:
			default:
			}
		}
		return t
	}
	return time.NewTimer(d)
}

// putTimer returns the timer to the pool.
// The timer must not be used after calling this function.
func putTimer(t *time.Timer) {
	if !t.Stop() {
		// Drain the channel if timer already fired
		select {
		case <-t.C:
		default:
		}
	}
	timerPool.Put(t)
}
