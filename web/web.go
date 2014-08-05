package web

import (
	"net/http"

	"github.com/Miniand/langtrend/web/options"
	"github.com/Miniand/langtrend/web/router"
	"github.com/gorilla/mux"
)

type Server struct {
	Options options.Options
	Router  *mux.Router
}

func New(opts options.Options) *Server {
	s := &Server{
		Options: opts,
		Router:  router.New(opts),
	}
	return s
}

func (s *Server) Run() error {
	http.Handle("/", s.Router)
	return http.ListenAndServe(s.Options.AddrWithDefault(), nil)
}
