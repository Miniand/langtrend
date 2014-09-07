package db

import (
	"fmt"
	"time"

	"github.com/Miniand/langtrend/period"
	"github.com/dancannon/gorethink"
)

const (
	GrandTotalField = "GRAND_TOTAL"
)

type Aggregate struct {
	Id         string    `gorethink:"id,omitempty"`
	Language   string    `gorethink:"language"`
	Type       string    `gorethink:"type"`
	Start      time.Time `gorethink:"start"`
	End        time.Time `gorethink:"end"`
	Total      int       `gorethink:"total"`
	TotalDirty bool      `gorethink:"total_dirty"`
	Ratio      float64   `gorethink:"ratio"`
	RatioDirty bool      `gorethink:"ratio_dirty"`
	Rank       int       `gorethink:"rank"`
	RankDirty  bool      `gorethink:"rank_dirty"`
}

func NewAggregate() Aggregate {
	return Aggregate{
		TotalDirty: true,
		RatioDirty: true,
		RankDirty:  true,
	}
}

func AggregateTable(kind string) string {
	return fmt.Sprintf("%s_aggregate", kind)
}

func (s *Session) EarliestAggregates(kind string) ([]Aggregate, error) {
	cur, err := s.Db().Table(AggregateTable(kind)).GroupByIndex(
		IndexName("language", "type")).Min("start").Ungroup().
		Map(func(row gorethink.Term) interface{} {
		return row.Field("reduction")
	}).OrderBy("language", "type").Run(s.Session)
	if err != nil {
		return nil, err
	}
	aggregates := []Aggregate{}
	err = cur.All(&aggregates)
	return aggregates, err
}

func (s *Session) LatestAggregates(kind string) ([]Aggregate, error) {
	cur, err := s.Db().Table(AggregateTable(kind)).GroupByIndex(
		IndexName("language", "type")).Max("start").Ungroup().
		Map(func(row gorethink.Term) interface{} {
		return row.Field("reduction")
	}).OrderBy("language", "type").Run(s.Session)
	if err != nil {
		return nil, err
	}
	aggregates := []Aggregate{}
	err = cur.All(&aggregates)
	return aggregates, err
}

func (s *Session) TotalDirtyAggregates(kind string) ([]Aggregate, error) {
	cur, err := s.Db().Table(AggregateTable(kind)).GetAllByIndex(
		"total_dirty",
		true,
	).Run(s.Session)
	if err != nil {
		return nil, err
	}
	aggregates := []Aggregate{}
	err = cur.All(&aggregates)
	return aggregates, err
}

func (s *Session) DirtyPeriods(kind, field string) ([]period.Perioder, error) {
	cur, err := s.Db().Table(AggregateTable(kind)).Filter(
		gorethink.Row.Field("language").Ne(GrandTotalField).And(
			gorethink.Row.Field(field).Eq(true))).
		Group("type", "start").Ungroup().
		Map(func(row gorethink.Term) interface{} {
		return map[string]interface{}{
			"type":  row.Field("group").Nth(0),
			"start": row.Field("group").Nth(1),
		}
	}).Run(s.Session)
	if err != nil {
		return nil, err
	}
	defer cur.Close()
	agg := Aggregate{}
	periods := []period.Perioder{}
	for cur.Next(&agg) {
		p, err := period.FromIdentifier(agg.Type)
		if err != nil {
			return nil, err
		}
		p.SetReference(agg.Start)
		periods = append(periods, p)
	}
	return periods, err
}

func (s *Session) RatioDirtyPeriods(kind string) ([]period.Perioder, error) {
	return s.DirtyPeriods(kind, "ratio_dirty")
}

func (s *Session) RankDirtyPeriods(kind string) ([]period.Perioder, error) {
	return s.DirtyPeriods(kind, "rank_dirty")
}

func (s *Session) SaveAggregate(kind string, aggregate Aggregate) (
	gorethink.WriteResponse, error) {
	if aggregate.Id == "" {
		return s.InsertAggregate(kind, aggregate)
	}
	return s.UpdateAggregate(kind, aggregate.Id, aggregate)
}

func (s *Session) InsertAggregate(kind string, aggregate Aggregate) (
	gorethink.WriteResponse, error) {
	return s.Db().Table(AggregateTable(kind)).Insert(aggregate).
		RunWrite(s.Session)
}

func (s *Session) UpdateAggregate(kind, id string, aggregate Aggregate) (
	gorethink.WriteResponse, error) {
	return s.Db().Table(AggregateTable(kind)).Get(id).Update(aggregate).
		RunWrite(s.Session)
}

func (s *Session) GrandTotalForPeriod(kind string, start, end time.Time) (int, error) {
	cur, err := s.Db().Table(kind).Between(start, end, gorethink.BetweenOpts{
		Index: "date",
	}).Sum("count").Run(s.Session)
	if err != nil {
		return 0, err
	}
	var sum int
	err = cur.One(&sum)
	return sum, err
}

func (s *Session) UpdateGrandTotalForPeriod(kind string, per period.Perioder) (int, error) {
	_, agg, err := s.FindAggregate(kind, GrandTotalField, per)
	if err != nil {
		return 0, err
	}
	start := per.Start()
	end := per.End()
	count, err := s.GrandTotalForPeriod(kind, start, end)
	if err != nil {
		return 0, err
	}
	agg.Total = count
	agg.TotalDirty = false
	_, err = s.SaveAggregate(kind, agg)
	return count, err
}

func (s *Session) AggregatesForPeriod(
	kind string,
	per period.Perioder,
) ([]Aggregate, error) {
	cur, err := s.Db().Table(AggregateTable(kind)).GetAllByIndex(
		IndexName("type", "start"),
		[]interface{}{per.Identifier(), per.Start()},
	).OrderBy(gorethink.Desc("total")).Run(s.Session)
	if err != nil {
		return nil, err
	}
	agg := []Aggregate{}
	err = cur.All(&agg)
	return agg, err
}

func (s *Session) UpdateRatiosForPeriod(kind string, per period.Perioder) error {
	aggregates, err := s.AggregatesForPeriod(kind, per)
	if err != nil {
		return err
	}
	foundTotal := false
	total := Aggregate{}
	for _, a := range aggregates {
		if a.Language == GrandTotalField {
			foundTotal = true
			total = a
			break
		}
	}
	if !foundTotal {
		return fmt.Errorf("could not find grand total for %s", per)
	}
	for _, a := range aggregates {
		if a.Language == GrandTotalField {
			continue
		}
		a.Ratio = float64(a.Total) / float64(total.Total)
		a.RatioDirty = false
		if _, err := s.SaveAggregate(kind, a); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) UpdateRanksForPeriod(kind string, per period.Perioder) error {
	aggregates, err := s.AggregatesForPeriod(kind, per)
	if err != nil {
		return err
	}
	rank := 1
	for _, a := range aggregates {
		if a.Language == GrandTotalField {
			continue
		}
		a.Rank = rank
		rank += 1
		a.RankDirty = false
		if _, err := s.SaveAggregate(kind, a); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) LanguageCountForPeriod(kind, language string,
	start, end time.Time) (int, error) {
	cur, err := s.Db().Table(kind).Between(
		[]interface{}{language, start},
		[]interface{}{language, end.Add(-time.Second)},
		gorethink.BetweenOpts{
			Index:      IndexName("language", "date"),
			RightBound: "closed",
		},
	).Sum("count").Run(s.Session)
	if err != nil {
		return 0, err
	}
	var sum int
	err = cur.One(&sum)
	return sum, err
}

func (s *Session) UpdateLanguageCountForPeriod(kind, language string,
	per period.Perioder) (int, error) {
	_, agg, err := s.FindAggregate(kind, language, per)
	if err != nil {
		return 0, err
	}
	start := per.Start()
	end := per.End()
	count, err := s.LanguageCountForPeriod(kind, language, start, end)
	if err != nil {
		return 0, err
	}
	agg.Total = count
	agg.TotalDirty = false
	_, err = s.SaveAggregate(kind, agg)
	return count, err
}

func (s *Session) FindAggregate(kind, language string,
	per period.Perioder) (found bool, agg Aggregate, err error) {
	agg = NewAggregate()
	cur, err := s.Db().Table(AggregateTable(kind)).GetAllByIndex(
		IndexName("language", "type", "start"),
		[]interface{}{language, per.Identifier(), per.Start()},
	).Limit(1).Run(s.Session)
	if err != nil {
		return
	}
	found = true
	err = cur.One(&agg)
	switch err {
	case nil:
		found = true
	case gorethink.ErrEmptyResult:
		err = nil
		agg.Language = language
		agg.Type = per.Identifier()
		agg.Start = per.Start()
		agg.End = per.End()
	}
	return
}

func (s *Session) TopRanked(kind, perType string) ([]Aggregate, error) {
	// Find the latest period with rankings
	cur, err := s.Db().Table(AggregateTable(kind)).GetAllByIndex(
		"type",
		perType,
	).Filter(gorethink.Row.Field("rank").Gt(0)).
		OrderBy(gorethink.Desc("start")).Limit(1).Run(s.Session)
	if err != nil {
		return nil, err
	}
	a := Aggregate{}
	if err = cur.One(&a); err != nil {
		return nil, err
	}
	// Get top ranked
	cur, err = s.Db().Table(AggregateTable(kind)).GetAllByIndex(
		IndexName("type", "start"),
		[]interface{}{perType, a.Start},
	).Filter(gorethink.Row.Field("language").Ne(GrandTotalField)).
		OrderBy("rank").Run(s.Session)
	if err != nil {
		return nil, err
	}
	agg := []Aggregate{}
	err = cur.All(&agg)
	return agg, err
}

func (s *Session) AggregatesForLanguageAndType(
	language, kind, perType string,
) ([]Aggregate, error) {
	cur, err := s.Db().Table(AggregateTable(kind)).GetAllByIndex(
		IndexName("language", "type"),
		[]interface{}{language, perType},
	).OrderBy("start").Run(s.Session)
	if err != nil {
		return nil, err
	}
	agg := []Aggregate{}
	err = cur.All(&agg)
	return agg, err
}

func (s *Session) CreateAggregateTable(kind string) error {
	table := AggregateTable(kind)
	if err := s.CreateTableIfNotExists(table); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(table, "language"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(table, "type"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(table, "start"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(table, "end"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(table, "total"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(table, "total_dirty"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(table, "language", "type"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(table, "type", "start"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(table, "language", "type", "start"); err != nil {
		return err
	}
	return nil
}
