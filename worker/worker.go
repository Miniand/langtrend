package worker

import (
	"log"
	"time"
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
		// Fetch created count for the next language in the queue.
		ran, err := w.FetchCreated()
		if err != nil {
			log.Printf("error fetching created, %s", err)
			continue
		}
		if ran {
			continue
		}
		// Fetch pushed count for the next language in the queue.
		ran, err = w.FetchPushed()
		if err != nil {
			log.Printf("error fetching pushed, %s", err)
			continue
		}
		if ran {
			continue
		}
	}
}
