package period

import (
	"time"
)

type Year struct {
	base
}

func (y Year) Identifier() string {
	return "month"
}

func (y Year) Start() time.Time {
	r := y.reference
	return time.Date(r.Year(), 1, 1, 0, 0, 0, 0, r.Location())
}

func (y Year) End() time.Time {
	return y.Start().AddDate(1, 0, 0)
}

func (y Year) String() string {
	return y.Start().Format("2006")
}
