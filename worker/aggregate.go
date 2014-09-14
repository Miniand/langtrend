package worker

import (
	"log"
	"time"

	"github.com/Miniand/langtrend/db"
	"github.com/Miniand/langtrend/period"
)

func periodTypes() []period.Perioder {
	return []period.Perioder{
		&period.Month{},
		&period.Quarter{},
		&period.Year{},
	}
}

func (w *Worker) UpdateLanguageTotal(kind, language string, per period.Perioder) error {
	count, err := w.Options.Db.UpdateLanguageCountForPeriod(kind, language, per)
	if err != nil {
		return err
	}
	log.Printf("%d %s repos %s during %s",
		count, language, kind, per)
	return nil
}

func (w *Worker) UpdateGrandTotal(kind string, per period.Perioder) error {
	count, err := w.Options.Db.UpdateGrandTotalForPeriod(kind, per)
	if err != nil {
		return err
	}
	log.Printf("Grand total of %d repos %s during %s",
		count, kind, per)
	return nil
}

func (w *Worker) Aggregate() (err error) {
	defer w.EnqueueAggregate(time.Now().Add(time.Hour))
	for _, kind := range []string{"created", "pushed"} {
		// Store the absolute earliest and latest counts for total creation
		earliestDate := time.Time{}
		latestDate := time.Time{}
		// Create later language totals
		latestCounts, err := w.Options.Db.LatestCounts(kind)
		latestAggregates, err := w.Options.Db.LatestAggregates(kind)
		if err != nil {
			return err
		}
		for _, l := range latestCounts {
			if latestDate.IsZero() || l.Date.After(latestDate) {
				latestDate = l.Date
			}
			for _, p := range periodTypes() {
				found := false
				existing := db.Aggregate{}
				for _, a := range latestAggregates {
					if a.Language == l.Language && a.Type == p.Identifier() {
						found = true
						existing = a
						break
					}
				}
				if !found {
					// Create one for the latest count
					p.SetReference(l.Date)
					if err := w.UpdateLanguageTotal(kind, l.Language, p); err != nil {
						return err
					}
					continue
				}
				// Update counts until current
				p.SetReference(existing.End)
				for !p.Start().After(l.Date) {
					if err := w.UpdateLanguageTotal(kind, l.Language, p); err != nil {
						return err
					}
					pStart := p.Start()
					if pStart.After(latestDate) {
						latestDate = pStart
					}
					p.SetReference(p.End())
				}
			}
		}
		// Create earlier language totals
		earliestCounts, err := w.Options.Db.EarliestCounts(kind)
		earliestAggregates, err := w.Options.Db.EarliestAggregates(kind)
		if err != nil {
			return err
		}
		for _, l := range earliestCounts {
			if earliestDate.IsZero() || l.Date.Before(earliestDate) {
				earliestDate = l.Date
			}
			for _, p := range periodTypes() {
				found := false
				existing := db.Aggregate{}
				for _, a := range earliestAggregates {
					if a.Language == l.Language && a.Type == p.Identifier() {
						found = true
						existing = a
						break
					}
				}
				if !found {
					continue
				}
				// Update counts until earliest
				p.SetReference(existing.Start.Add(-time.Second))
				for !p.End().Before(l.Date) && !p.End().Equal(l.Date) {
					if err := w.UpdateLanguageTotal(kind, l.Language, p); err != nil {
						return err
					}
					pStart := p.Start()
					if pStart.Before(earliestDate) {
						earliestDate = pStart
					}
					p.SetReference(p.Start().Add(-time.Second))
				}
			}
		}
		// Update dirty language totals
		totalDirty, err := w.Options.Db.TotalDirtyAggregates(kind)
		if err != nil {
			return err
		}
		for _, a := range totalDirty {
			if a.Language == db.GrandTotalField {
				continue
			}
			p, err := period.FromIdentifier(a.Type)
			if err != nil {
				return err
			}
			p.SetReference(a.Start)
			if err := w.UpdateLanguageTotal(kind, a.Language, p); err != nil {
				return err
			}
		}
		// Create later grand totals
		if earliestDate.IsZero() || latestDate.IsZero() {
			return nil
		}
		for _, p := range periodTypes() {
			found := false
			existing := db.Aggregate{}
			for _, a := range latestAggregates {
				if a.Language == db.GrandTotalField && a.Type == p.Identifier() {
					found = true
					existing = a
					break
				}
			}
			if !found {
				// Create one for the latest count
				p.SetReference(latestDate)
				if err := w.UpdateGrandTotal(kind, p); err != nil {
					return err
				}
				continue
			}
			// Update counts until current
			p.SetReference(existing.End)
			for !p.Start().After(latestDate) {
				if err := w.UpdateGrandTotal(kind, p); err != nil {
					return err
				}
				p.SetReference(p.End())
			}
		}
		// Create earlier grand ratios
		for _, p := range periodTypes() {
			found := false
			existing := db.Aggregate{}
			for _, a := range earliestAggregates {
				if a.Language == db.GrandTotalField && a.Type == p.Identifier() {
					found = true
					existing = a
					break
				}
			}
			if !found {
				continue
			}
			// Update counts until earliest
			p.SetReference(existing.Start.Add(-time.Second))
			for !p.End().Before(earliestDate) && !p.End().Equal(earliestDate) {
				if err := w.UpdateGrandTotal(kind, p); err != nil {
					return err
				}
				p.SetReference(p.Start().Add(-time.Second))
			}
		}
		// Update dirty grand totals
		for _, a := range totalDirty {
			if a.Language != db.GrandTotalField {
				continue
			}
			p, err := period.FromIdentifier(a.Type)
			if err != nil {
				return err
			}
			p.SetReference(a.Start)
			if err := w.UpdateGrandTotal(kind, p); err != nil {
				return err
			}
		}
		// Update dirty ratios
		ratioDirty, err := w.Options.Db.RatioDirtyPeriods(kind)
		if err != nil {
			return err
		}
		for _, p := range ratioDirty {
			log.Printf("Updating ratios for %s", p)
			if err := w.Options.Db.UpdateRatiosForPeriod(kind, p); err != nil {
				return err
			}
		}
		// Update dirty ranks
		rankDirty, err := w.Options.Db.RankDirtyPeriods(kind)
		if err != nil {
			return err
		}
		for _, p := range rankDirty {
			log.Printf("Updating ranks for %s", p)
			if err := w.Options.Db.UpdateRanksForPeriod(kind, p); err != nil {
				return err
			}
		}
	}
	return
}
