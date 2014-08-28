package worker

import (
	"fmt"
	"log"
	"time"

	"github.com/Miniand/langtrend/github"
)

func (w *Worker) RunFetch() (ran bool, err error) {
	// Fetch created count for the next language in the queue.
	ran, err = w.FetchNextDateVal("created")
	if ran || err != nil {
		return
	}
	// Fetch pushed count for the next language in the queue.
	ran, err = w.FetchNextDateVal("pushed")
	return
}

func (w *Worker) FetchDateVal(kind, language string, date time.Time) error {
	count, err := github.GetCountOnDateForLang(date, kind, language,
		w.Options.Username, w.Options.Password)
	if err != nil {
		return fmt.Errorf("unable to fetch from GitHub, %v", err)
	}
	log.Printf("%d %s repos %s on %s\n",
		count, language, kind, github.FormatDate(date))
	return w.Options.Db.SaveLanguageCount(kind, language, date, count)
}

func (w *Worker) FetchNextDateVal(kind string) (ran bool, err error) {
	earliest := w.Options.earliest()
	latest := time.Now().Add(2 * -24 * time.Hour)
	// Check if a language doesn't have eny entries yet.
	existingLangs := map[string]bool{}
	langs, err := w.Options.Db.LanguageList(kind)
	if err != nil {
		return false, err
	}
	for _, l := range langs {
		existingLangs[l] = true
	}
	for _, l := range github.Languages {
		if !existingLangs[l] {
			return true, w.FetchDateVal(kind, l, latest)
		}
	}
	// Check if a language doesn't have recent entries.
	ldc, found, err := w.Options.Db.LastLanguageCount(kind)
	if err != nil {
		return false, err
	}
	if found && ldc.Date.Before(latest) {
		if err != nil {
			return true, fmt.Errorf(`unable to get the time from lcd, %v`, err)
		}
		return true, w.FetchDateVal(kind, ldc.Language,
			ldc.Date.Add(time.Duration(24)*time.Hour))
	}
	// Check if a language doesn't have past entries.
	ldc, found, err = w.Options.Db.FirstLanguageCount(kind)
	if err != nil {
		return false, err
	}
	if found && ldc.Date.After(earliest) {
		if err != nil {
			return true, fmt.Errorf(`unable to get the time from lcd, %v`, err)
		}
		return true, w.FetchDateVal(kind, ldc.Language,
			ldc.Date.Add(time.Duration(-24)*time.Hour))
	}
	return false, nil
}
