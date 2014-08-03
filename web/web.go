package web

import (
	"net/http"

	"github.com/Miniand/langtrend/db"
	"github.com/gorilla/mux"
)

type Options struct {
	Db   *db.Session
	Addr string
}

type Server struct {
	Options Options
	Router  *mux.Router
}

func New(options Options) *Server {
	s := &Server{
		Options: options,
		Router:  mux.NewRouter(),
	}
	s.Router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello"))
	})
	return s
}

func (s *Server) Run() error {
	http.Handle("/", s.Router)
	return http.ListenAndServe(s.Options.Addr, nil)
}
