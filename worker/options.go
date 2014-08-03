package worker

import (
	"time"

	"github.com/Miniand/langtrend/db"
)

type Options struct {
	Db       *db.Session
	Rate     time.Duration
	Username string
	Password string
	Earliest time.Time
}

func (o Options) rate() time.Duration {
	if o.Rate != 0 {
		return o.Rate
	}
	if o.Username != "" && o.Password != "" {
		// Authenticated rate limit is 20 requests per minute.
		return time.Minute / 20
	}
	// Unauthenticated rate limit is 5 requests per minute.
	return time.Minute / 5
}

func (o Options) earliest() time.Time {
	if !o.Earliest.IsZero() {
		return o.Earliest
	}
	// Wikipedia states that GitHub was launched on 2008-04-01.
	return time.Date(2008, time.April, 1, 0, 0, 0, 0, time.UTC)
}
