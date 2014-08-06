package router

import (
	"log"
	"net/http"

	"github.com/Miniand/langtrend/web/options"
	"github.com/Miniand/langtrend/web/view"
	"github.com/gorilla/mux"
)

func Language(opts options.Options) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		counts, err := opts.Db.LanguageCountsByWeek(vars["language"], "created")
		if err != nil {
			log.Fatalf("could not get language counts for %s, %v",
				vars["language"], err)
		}
		view.LanguageShow(w, view.LanguageShowData{
			Name:   vars["language"],
			Counts: counts,
		})
	}
}
