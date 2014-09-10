package worker

import (
	"fmt"
	"log"
	"time"

	"github.com/Miniand/langtrend/db"
	"github.com/Miniand/langtrend/github"
	"github.com/dancannon/gorethink/encoding"
)

const (
	JobCreateGHJobs = "createGHJobs"
	JobFetch        = "fetch"
	JobAggregate    = "aggregate"
)

type FetchJobArgs struct {
	Kind     string    `gorethink:"kind"`
	Language string    `gorethink:"language"`
	Date     time.Time `gorethink:"date"`
}

func (w *Worker) EnqueueCreateGHJobs(after time.Time) error {
	return w.Options.Db.Enqueue(db.Job{
		Id:    JobCreateGHJobs,
		Type:  JobCreateGHJobs,
		After: after,
	})
}

func (w *Worker) EnqueueFetch(kind, language string, date time.Time) error {
	return w.Options.Db.Enqueue(db.Job{
		Id:   fmt.Sprintf("%s:%s:%s", kind, language, github.FormatDate(date)),
		Type: JobFetch,
		Args: FetchJobArgs{
			Kind:     kind,
			Language: language,
			Date:     date,
		},
	})
}

func (w *Worker) EnqueueAggregate(after time.Time) error {
	return w.Options.Db.Enqueue(db.Job{
		Id:    JobAggregate,
		Type:  JobAggregate,
		After: after,
	})
}

func (w *Worker) RunJob(job db.Job) error {
	switch job.Type {
	case JobCreateGHJobs:
		return w.RunCreateGHJobs()
	case JobAggregate:
		return w.Aggregate()
	case JobFetch:
		args := FetchJobArgs{}
		if err := encoding.Decode(&args, job.Args); err != nil {
			return fmt.Errorf("error decoding FetchJobArgs, %s", err)
		}
		return w.FetchDateVal(args.Kind, args.Language, args.Date)
	}
	return fmt.Errorf("unable to understand job %v", job)
}

func floorDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}
func nextDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location())
}
func prevDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day()-1, 0, 0, 0, 0, t.Location())
}

func (w *Worker) RunCreateGHJobs() error {
	log.Print("Creating GitHub jobs, this may take some time")
	defer w.EnqueueCreateGHJobs(time.Now().Add(6 * time.Hour))
	maxDate := floorDay(time.Now().Add(2 * -24 * time.Hour))
	minDate := w.Options.Earliest
	for _, kind := range []string{"created", "pushed"} {
		// Prepare data based on earliest and latest language dates.
		earliestCounts, err := w.Options.Db.EarliestCounts(kind)
		if err != nil {
			return fmt.Errorf("error getting earlist %s counts, %s", kind, err)
		}
		latestCounts, err := w.Options.Db.LatestCounts(kind)
		if err != nil {
			return fmt.Errorf("error getting earlist %s counts, %s", kind, err)
		}
		firstLatest := time.Time{}
		earliestByLang := map[string]time.Time{}
		latestByLang := map[string]time.Time{}
		for _, c := range earliestCounts {
			earliestByLang[c.Language] = c.Date
		}
		for _, c := range latestCounts {
			latestByLang[c.Language] = c.Date
			if firstLatest.IsZero() || c.Date.Before(firstLatest) {
				firstLatest = c.Date
			}
		}
		if firstLatest.IsZero() {
			firstLatest = maxDate
		}
		cur := firstLatest
		for cur.Before(maxDate) || cur.Equal(maxDate) {
			for _, lang := range github.Languages {
				if lat, ok := latestByLang[lang]; !ok || lat.Before(cur) {
					if err := w.EnqueueFetch(kind, lang, cur); err != nil {
						return fmt.Errorf(
							"error enqueuing fetch %s for %s on %s, %s",
							kind,
							lang,
							cur,
							err,
						)
					}
				}
			}
			cur = nextDay(cur)
		}
		cur = prevDay(firstLatest)
		for cur.After(minDate) || cur.Equal(minDate) {
			for _, lang := range github.Languages {
				if ear, ok := earliestByLang[lang]; !ok || ear.After(cur) {
					if err := w.EnqueueFetch(kind, lang, cur); err != nil {
						return fmt.Errorf(
							"error enqueuing fetch %s for %s on %s, %s",
							kind,
							lang,
							cur,
							err,
						)
					}
				}
			}
			cur = prevDay(cur)
		}
	}
	log.Print("Created GitHub jobs")
	return nil
}
