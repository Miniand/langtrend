package worker

import (
	"log"
	"time"

	"github.com/Miniand/langtrend/github"
)

type Worker struct {
	Options Options
}

func New(options Options) *Worker {
	return &Worker{options}
}

func (w *Worker) Run() {
	for {
		time.Sleep(w.Options.rate())
		ls, err := w.Options.Db.NextFetchLanguage()
		if err != nil {
			log.Printf("error fetching next query language: %s\n", err)
			continue
		}
		lang := ls.Id
		date := time.Now().Add(2 * -24 * time.Hour)
		formattedDate := github.Format(date)
		if ls.Count > 0 {
			var (
				parseDate string
				offset    time.Duration
			)
			if ls.Max < formattedDate {
				parseDate = ls.Max
				offset = 24 * time.Hour
			} else if ls.Min > github.Format(w.Options.earliest()) {
				parseDate = ls.Min
				offset = -24 * time.Hour
			} else {
				continue
			}
			date, err = time.Parse(github.DateFormat, parseDate)
			if err != nil {
				log.Printf("error parsing date %s: %s\n", parseDate, err)
				continue
			}
			date = date.Add(offset)
			formattedDate = github.Format(date)
		}
		created, err := github.GetCreatedOnDateForLang(
			date,
			lang,
			w.Options.Username,
			w.Options.Password)
		if err == nil {
			log.Printf("got %d for %s on %s",
				created, lang, formattedDate)
			if err := w.Options.Db.SaveLanguageCount(lang, date, created); err != nil {
				log.Printf("error saving lang count, %s\n", err)
			}
		} else {
			log.Printf("error querying created count for %s on %s: %s\n",
				ls.Id, formattedDate, err)
		}
	}
}
