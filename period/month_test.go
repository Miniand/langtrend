package period

import (
	"testing"
	"time"
)

func TestMonth_Start(t *testing.T) {
	m := Month{}
	m.SetReference(time.Date(2012, 6, 8, 4, 6, 8, 4, time.UTC))
	assertEquals(t, time.Date(2012, 6, 1, 0, 0, 0, 0, time.UTC), m.Start())
}

func TestMonth_End(t *testing.T) {
	m := Month{}
	m.SetReference(time.Date(2012, 6, 8, 4, 6, 8, 4, time.UTC))
	assertEquals(t, time.Date(2012, 7, 1, 0, 0, 0, 0, time.UTC), m.End())
}
