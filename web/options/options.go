package options

import (
	"fmt"

	"github.com/Miniand/langtrend/db"
)

type Options struct {
	Db   *db.Session
	Port int
	Addr string
}

func (o Options) AddrWithDefault() string {
	if o.Addr != "" {
		return o.Addr
	}
	if o.Port != 0 {
		return fmt.Sprintf(":%d", o.Port)
	}
	return ":3000"
}
