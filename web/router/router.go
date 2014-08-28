package router

import (
	"github.com/Miniand/langtrend/web/options"
	"github.com/gorilla/mux"
)

func New(opts options.Options) *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/l/{language}", Language(opts))
	r.HandleFunc("/l", Language(opts))
	r.HandleFunc("/", Home(opts))
	return r
}
