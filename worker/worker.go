package worker

import "log"

type Worker struct {
	Options Options
}

func New(options Options) *Worker {
	return &Worker{options}
}

func (w *Worker) Run() {
	for {
		if w.Options.Aggregate {
			ran, err := w.RunAggregate()
			if err != nil {
				log.Printf("error running aggregate, %s", err)
			}
			if ran {
				continue
			}
		}
		ran, err := w.RunFetch()
		if err != nil {
			log.Printf("error running fetch, %s", err)
		}
		if ran {
			continue
		}
	}
}
