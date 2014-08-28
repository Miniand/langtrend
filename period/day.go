package period

import (
	"time"
)

type Day struct {
	base
}

func (d Day) Identifier() string {
	return "day"
}

func (d Day) Start() time.Time {
	r := d.reference
	return time.Date(r.Year(), r.Month(), r.Day(), 0, 0, 0, 0, r.Location())
}

func (d Day) End() time.Time {
	return d.Start().AddDate(0, 0, 1)
}

func (d Day) String() string {
	return d.Start().Format("02 Jan 2006")
}
