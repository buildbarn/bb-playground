package sync

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/util"
)

// ConditionVariable implements a condition variable, similar to
// sync.Cond. The difference is that its Wait() operation takes a
// context, allowing the waiting to be interrupted.
type ConditionVariable struct {
	wakeup chan struct{}
}

// Broadcast on the condition variable, waking up any goroutines that
// are waiting.
func (c *ConditionVariable) Broadcast() {
	if c.wakeup != nil {
		close(c.wakeup)
		c.wakeup = nil
	}
}

// Wait for a broadcast to occur on the condition variable. Upon
// success, the provided lock will be reacquired. Upon failure, the lock
// will be left unlocked.
func (c *ConditionVariable) Wait(ctx context.Context, l sync.Locker) error {
	if c.wakeup == nil {
		c.wakeup = make(chan struct{})
	}
	wakeup := c.wakeup
	l.Unlock()
	select {
	case <-ctx.Done():
		return util.StatusFromContext(ctx)
	case <-wakeup:
		l.Lock()
		return nil
	}
}
