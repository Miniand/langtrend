package db

import (
	"fmt"
	"time"

	"github.com/Miniand/langtrend/github"
	"github.com/dancannon/gorethink"
)

const (
	TableCreated = "created"
	TablePushed  = "pushed"
)

type LanguageDateCount struct {
	Id       string    `gorethink:"id,omitempty"`
	Language string    `gorethink:"language"`
	Date     time.Time `gorethink:"date"`
	Count    int       `gorethink:"count"`
}

func (s *Session) LanguageList(table string) ([]string, error) {
	cur, err := s.Db().Table(table).Distinct(gorethink.DistinctOpts{
		Index: "language",
	}).Run(s.Session)
	if err != nil {
		return nil, err
	}
	defer cur.Close()
	languages := []string{}
	err = cur.All(&languages)
	return languages, err
}

func (s *Session) CreateCountTable(kind string) error {
	if err := s.CreateTableIfNotExists(kind); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(kind, "language"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(kind, "date"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(kind, "language", "date"); err != nil {
		return err
	}
	return nil
}

func (s *Session) SaveLanguageCount(
	kind, language string,
	date time.Time,
	count int,
) error {
	// Update total.
	_, err := s.Db().Table(kind).Insert(LanguageDateCount{
		Id:       fmt.Sprintf("%s-%s", language, github.FormatDate(date)),
		Language: language,
		Date:     date,
		Count:    count,
	}, gorethink.InsertOpts{
		Conflict: "update",
	}).RunWrite(s.Session)
	if err != nil {
		return err
	}
	// Mark aggregate totals for this language and grand as dirty.
	_, err = s.Db().Table(AggregateTable(kind)).GetAllByIndex(
		"language",
		language,
		GrandTotalField,
	).Filter(gorethink.Expr(date).During(
		gorethink.Row.Field("start"),
		gorethink.Row.Field("end"),
	)).Update(map[string]interface{}{
		"total_dirty": true,
	}).RunWrite(s.Session)
	if err != nil {
		return err
	}
	// Mark aggregate ratios and ranks for everything as dirty.
	_, err = s.Db().Table(AggregateTable(kind)).Filter(
		gorethink.Expr(date).During(
			gorethink.Row.Field("start"),
			gorethink.Row.Field("end"),
		)).Update(map[string]interface{}{
		"ratio_dirty": true,
		"rank_dirty":  true,
	}).RunWrite(s.Session)
	return err
}

func (s *Session) LastLanguageCount(kind string) (
	ldc LanguageDateCount, found bool, err error) {
	cur, err := s.Db().Table(kind).GroupByIndex("language").Max("date").
		Ungroup().Map(func(row gorethink.Term) interface{} {
		return row.Field("reduction")
	}).OrderBy("date").Limit(10).Sample(1).Run(s.Session)
	if err != nil {
		return
	}
	defer cur.Close()
	if cur.IsNil() {
		return
	}
	found = true
	err = cur.One(&ldc)
	return
}

func (s *Session) FirstLanguageCount(kind string) (
	ldc LanguageDateCount, found bool, err error) {
	cur, err := s.Db().Table(kind).GroupByIndex("language").Min("date").
		Ungroup().Map(func(row gorethink.Term) interface{} {
		return row.Field("reduction")
	}).OrderBy(gorethink.Desc("date")).Limit(10).Sample(1).Run(s.Session)
	if err != nil {
		return
	}
	defer cur.Close()
	if cur.IsNil() {
		return
	}
	found = true
	err = cur.One(&ldc)
	return
}

func (s *Session) EarliestCounts(table string) ([]LanguageDateCount, error) {
	cur, err := s.Db().Table(table).GroupByIndex("language").Min("date").
		Ungroup().Map(func(row gorethink.Term) interface{} {
		return row.Field("reduction")
	}).OrderBy("language").Run(s.Session)
	if err != nil {
		return nil, err
	}
	defer cur.Close()
	counts := []LanguageDateCount{}
	err = cur.All(&counts)
	return counts, err
}

func (s *Session) LatestCounts(table string) ([]LanguageDateCount, error) {
	cur, err := s.Db().Table(table).GroupByIndex("language").Max("date").
		Ungroup().Map(func(row gorethink.Term) interface{} {
		return row.Field("reduction")
	}).OrderBy("language").Run(s.Session)
	if err != nil {
		return nil, err
	}
	defer cur.Close()
	counts := []LanguageDateCount{}
	err = cur.All(&counts)
	return counts, err
}
