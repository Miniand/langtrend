package worker

import (
	"time"

	"github.com/Miniand/langtrend/db"
)

type Options struct {
	Db       *db.Session
	Username string
	Password string
	Earliest time.Time
}

func (o Options) earliest() time.Time {
	if !o.Earliest.IsZero() {
		return o.Earliest
	}
	// Wikipedia states that GitHub was launched on 2008-04-01.
	return time.Date(2008, time.April, 1, 0, 0, 0, 0, time.UTC)
}
