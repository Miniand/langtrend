package router

import (
	"net/http"

	"github.com/Miniand/langtrend/web/options"
	"github.com/Miniand/langtrend/web/view"
)

func Home(opts options.Options) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		view.Home(w, view.HomeData{})
	}
}
