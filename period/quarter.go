package period

import (
	"fmt"
	"time"
)

type Quarter struct {
	base
}

func (q Quarter) Identifier() string {
	return "quarter"
}

func (q Quarter) Start() time.Time {
	r := q.reference
	return time.Date(r.Year(), time.Month((q.Num()-1)*3+1), 1, 0, 0, 0, 0,
		r.Location())
}

func (q Quarter) End() time.Time {
	return q.Start().AddDate(0, 3, 0)
}

func (q Quarter) Num() int {
	return int(q.reference.Month()-1)/3 + 1
}

func (q Quarter) String() string {
	return fmt.Sprintf("Q%d %d", q.Num(), q.reference.Year())
}
