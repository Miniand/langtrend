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
		query := r.URL.Query()
		axisPeriod := query.Get("period")
		if axisPeriod == "" {
			axisPeriod = "month"
		}
		kind := query.Get("kind")
		if kind == "" {
			kind = "created"
		}
		languages := strings.Split(vars["language"], ",")
		if len(languages) == 1 && languages[0] == "" {
			languages = []string{}
			langs, err := opts.Db.TopRanked(kind, axisPeriod)
			if err != nil {
				log.Fatalf("could not get top ranked, %s", err)
			}
			until := 10
			if l := len(langs); l < until {
				until = l
			}
			for i := 0; i < until; i++ {
				languages = append(languages, langs[i].Language)
			}
		}
		counts := [][]db.Aggregate{}
		for _, l := range languages {
			c, err := opts.Db.AggregatesForLanguageAndType(l, kind, axisPeriod)
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
