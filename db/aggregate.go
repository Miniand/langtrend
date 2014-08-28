package db

import (
	"fmt"
	"time"

	"github.com/Miniand/langtrend/period"
	"github.com/dancannon/gorethink"
)

const (
	TableCreatedAggregate = "created_aggregate"
	TablePushedAggregate  = "pushed_aggregate"
	GrandTotalField       = "GRAND_TOTAL"
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
	cur, err := s.Db().Table(AggregateTable(kind)).Group("language", "type").
		Min("start").Ungroup().Map(func(row gorethink.Term) interface{} {
		return row.Field("reduction")
	}).OrderBy("language", "type").Run(s.Session)
	if err != nil {
		return nil, err
	}
	defer cur.Close()
	aggregates := []Aggregate{}
	err = cur.All(&aggregates)
	return aggregates, err
}

func (s *Session) LatestAggregates(kind string) ([]Aggregate, error) {
	cur, err := s.Db().Table(AggregateTable(kind)).Group("language", "type").
		Max("start").Ungroup().Map(func(row gorethink.Term) interface{} {
		return row.Field("reduction")
	}).OrderBy("language", "type").Run(s.Session)
	if err != nil {
		return nil, err
	}
	defer cur.Close()
	aggregates := []Aggregate{}
	err = cur.All(&aggregates)
	return aggregates, err
}

func (s *Session) TotalDirtyAggregates(kind string) ([]Aggregate, error) {
	cur, err := s.Db().Table(AggregateTable(kind)).Filter(
		gorethink.Row.Field("total_dirty").Eq(true)).Run(s.Session)
	if err != nil {
		return nil, err
	}
	defer cur.Close()
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
	defer cur.Close()
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
	cur, err := s.Db().Table(AggregateTable(kind)).Filter(
		gorethink.Row.Field("type").Eq(per.Identifier()).And(
			gorethink.Row.Field("start").Eq(per.Start()))).
		OrderBy(gorethink.Desc("total")).Run(s.Session)
	if err != nil {
		return nil, err
	}
	defer cur.Close()
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
	cur, err := s.Db().Table(kind).GetAllByIndex("language", language).Filter(
		gorethink.Row.Field("date").Ge(start).And(
			gorethink.Row.Field("date").Lt(end))).Sum("count").Run(s.Session)
	if err != nil {
		return 0, err
	}
	defer cur.Close()
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
	cur, err := s.Db().Table(AggregateTable(kind)).Filter(
		gorethink.Row.Field("language").Eq(language).And(
			gorethink.Row.Field("type").Eq(per.Identifier())).And(
			gorethink.Row.Field("start").Eq(per.Start()))).Run(s.Session)
	if err != nil {
		return
	}
	defer cur.Close()
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

func (s *Session) CreateCreatedAggregateTable() error {
	if err := s.CreateTableIfNotExists(TableCreatedAggregate); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(TableCreatedAggregate, "language"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(TableCreatedAggregate, "type"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(TableCreatedAggregate, "start"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(TableCreatedAggregate, "end"); err != nil {
		return err
	}
	return s.CreateIndexIfNotExists(TableCreatedAggregate, "total")
}

func (s *Session) CreatePushedAggregateTable() error {
	if err := s.CreateTableIfNotExists(TablePushedAggregate); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(TablePushedAggregate, "language"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(TablePushedAggregate, "type"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(TablePushedAggregate, "start"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(TablePushedAggregate, "end"); err != nil {
		return err
	}
	return s.CreateIndexIfNotExists(TablePushedAggregate, "total")
}
