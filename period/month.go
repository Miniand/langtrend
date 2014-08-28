package period

import (
	"time"
)

type Month struct {
	base
}

func (m Month) Identifier() string {
	return "month"
}

func (m Month) Start() time.Time {
	r := m.reference
	return time.Date(r.Year(), r.Month(), 1, 0, 0, 0, 0, r.Location())
}

func (m Month) End() time.Time {
	return m.Start().AddDate(0, 1, 0)
}

func (m Month) String() string {
	return m.Start().Format("Jan 2006")
}
