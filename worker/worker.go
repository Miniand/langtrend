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
		// Fetch created count for the next language in the queue.
		ran, err := w.FetchNextDateVal("created")
		if err != nil {
			log.Printf("error fetching created, %s", err)
			continue
		}
		if ran {
			continue
		}
		// Fetch pushed count for the next language in the queue.
		ran, err = w.FetchNextDateVal("pushed")
		if err != nil {
			log.Printf("error fetching pushed, %s", err)
			continue
		}
		if ran {
			continue
		}
	}
}
