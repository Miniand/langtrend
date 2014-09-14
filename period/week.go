package period

import (
	"fmt"
	"time"
)

type Week struct {
	base
}

func (w Week) Identifier() string {
	return "week"
}

func (w Week) Start() time.Time {
	r := w.reference
	return time.Date(r.Year(), r.Month(), r.Day()-int(r.Weekday()), 0, 0, 0, 0,
		r.Location())
}

func (w Week) End() time.Time {
	return w.Start().AddDate(0, 0, 7)
}

func (w Week) Num() int {
	return int(w.End().YearDay()-2)/7 + 1
}

func (w Week) String() string {
	return fmt.Sprintf("W%d %d", w.Num(), w.End().AddDate(0, 0, -1).Year())
}
