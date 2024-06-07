package sync

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/util"
)

type ConditionVariable struct {
	wakeup chan struct{}
}

func (c *ConditionVariable) Broadcast() {
	if c.wakeup != nil {
		close(c.wakeup)
		c.wakeup = nil
	}
}

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
