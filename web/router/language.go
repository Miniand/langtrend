package router

import (
	"log"
	"net/http"
	"strings"

	"github.com/Miniand/langtrend/db"
	"github.com/Miniand/langtrend/web/options"
	"github.com/Miniand/langtrend/web/view"
	"github.com/gorilla/mux"
)

func Language(opts options.Options) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		languages := strings.Split(vars["language"], ",")
		counts := [][]db.LanguageDateCount{}
		for _, l := range languages {
			c, err := opts.Db.LanguageCountsByWeek(l, "created")
			if err != nil {
				log.Fatalf("could not get language counts for %s, %v",
					vars["language"], err)
			}
			counts = append(counts, c)
		}
		if err := view.LanguageShow(w, view.LanguageShowData{
			Name:   vars["language"],
			Counts: counts,
		}); err != nil {
			log.Fatalf("error rendering language show, %v", err)
		}
	}
}
