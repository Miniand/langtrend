package period

import (
	"testing"
	"time"
)

func TestQuarter_Start(t *testing.T) {
	q := Quarter{}
	q.SetReference(time.Date(2012, 6, 8, 4, 6, 8, 4, time.UTC))
	assertEquals(t, time.Date(2012, 4, 1, 0, 0, 0, 0, time.UTC), q.Start())
}

func TestQuarter_End(t *testing.T) {
	q := Quarter{}
	q.SetReference(time.Date(2012, 6, 8, 4, 6, 8, 4, time.UTC))
	assertEquals(t, time.Date(2012, 7, 1, 0, 0, 0, 0, time.UTC), q.End())
}

func TestQuarter_Num(t *testing.T) {
	q := Quarter{}
	q.SetReference(time.Date(2012, 6, 8, 4, 6, 8, 4, time.UTC))
	assertEquals(t, 2, q.Num())
}

func TestQuarter_String(t *testing.T) {
	q := Quarter{}
	q.SetReference(time.Date(2012, 6, 8, 4, 6, 8, 4, time.UTC))
	assertEquals(t, "Q2 2012", q.String())
}
