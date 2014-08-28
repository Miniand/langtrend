package period

import (
	"testing"
	"time"
)

func TestYear_Start(t *testing.T) {
	y := Year{}
	y.SetReference(time.Date(2012, 6, 8, 4, 6, 8, 4, time.UTC))
	assertEquals(t, time.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC), y.Start())
}

func TestYear_End(t *testing.T) {
	y := Year{}
	y.SetReference(time.Date(2012, 6, 8, 4, 6, 8, 4, time.UTC))
	assertEquals(t, time.Date(2013, 1, 1, 0, 0, 0, 0, time.UTC), y.End())
}
