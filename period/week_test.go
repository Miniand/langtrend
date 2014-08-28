package period

import (
	"testing"
	"time"
)

func TestWeek_Start(t *testing.T) {
	w := Week{}
	w.SetReference(time.Date(2012, 6, 8, 4, 6, 8, 4, time.UTC))
	assertEquals(t, time.Date(2012, 6, 3, 0, 0, 0, 0, time.UTC), w.Start())
}

func TestWeek_End(t *testing.T) {
	w := Week{}
	w.SetReference(time.Date(2012, 6, 8, 4, 6, 8, 4, time.UTC))
	assertEquals(t, time.Date(2012, 6, 10, 0, 0, 0, 0, time.UTC), w.End())
}

func TestWeek_Num(t *testing.T) {
	w := Week{}
	w.SetReference(time.Date(2012, 6, 8, 4, 6, 8, 4, time.UTC))
	assertEquals(t, 23, w.Num())
}

func TestWeek_String(t *testing.T) {
	w := Week{}
	w.SetReference(time.Date(2012, 6, 8, 4, 6, 8, 4, time.UTC))
	assertEquals(t, "W23 2012", w.String())
}
