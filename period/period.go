package period

import (
	"fmt"
	"time"
)

type Perioder interface {
	SetReference(time.Time)
	Reference() time.Time
	Identifier() string
	Start() time.Time
	End() time.Time
	String() string
}

type base struct {
	reference time.Time
}

func (b *base) SetReference(t time.Time) {
	b.reference = t
}

func (b base) Reference() time.Time {
	return b.reference
}

func Types() []Perioder {
	return []Perioder{
		&Day{},
	}
}

func FromIdentifier(identifier string) (Perioder, error) {
	for _, t := range Types() {
		if t.Identifier() == identifier {
			return t, nil
		}
	}
	return nil, fmt.Errorf("could not find period with identifier '%s'",
		identifier)
}
