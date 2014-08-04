package worker

import (
	"fmt"
	"log"
	"time"

	"github.com/Miniand/langtrend/github"
)

func (w *Worker) FetchCreated() (ran bool, err error) {
	earliest := w.Options.earliest()
	latest := time.Now().Add(2 * -24 * time.Hour)
	ls, found, err := w.Options.Db.NextLanguageToFetchCreated(
		earliest, latest)
	if err != nil {
		return false, fmt.Errorf(
			"error fetching next query language, %s", err)
	}
	if found {
		lang := ls.Id
		date, err := ls.NextFetchDate()
		formattedDate := github.FormatDate(date)
		if err != nil {
			return false, fmt.Errorf(
				"error getting next fetch date, %s", err)
		}
		created, err := github.GetCreatedOnDateForLang(
			date,
			lang,
			w.Options.Username,
			w.Options.Password)
		if err != nil {
			return false, fmt.Errorf(
				"error querying created count for %s on %s, %s",
				ls.Id, formattedDate, err)
		}
		log.Printf("got %d created for %s on %s",
			created, lang, formattedDate)
		if err := w.Options.Db.SaveLanguageCreatedCount(lang, date, created); err != nil {
			log.Printf("error saving lang count, %s", err)
		}
		ran = true
	}
	return
}
