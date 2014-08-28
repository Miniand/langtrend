package period

import (
	"testing"
	"time"
)

func TestDay_Start(t *testing.T) {
	d := Day{}
	d.SetReference(time.Date(2012, 6, 8, 4, 6, 8, 4, time.UTC))
	assertEquals(t, time.Date(2012, 6, 8, 0, 0, 0, 0, time.UTC), d.Start())
}

func TestDay_End(t *testing.T) {
	d := Day{}
	d.SetReference(time.Date(2012, 6, 8, 4, 6, 8, 4, time.UTC))
	assertEquals(t, time.Date(2012, 6, 9, 0, 0, 0, 0, time.UTC), d.End())
}
