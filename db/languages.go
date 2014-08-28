package db

import (
	"time"

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
	cur, err := s.Db().Table(table).Pluck("language").Field("language").Run(
		s.Session)
	if err != nil {
		return nil, err
	}
	defer cur.Close()
	languages := []string{}
	err = cur.All(&languages)
	return languages, err
}

func (s *Session) CreateCreatedTable() error {
	if err := s.CreateTableIfNotExists(TableCreated); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(TableCreated, "language"); err != nil {
		return err
	}
	return s.CreateIndexIfNotExists(TableCreated, "date")
}

func (s *Session) CreatePushedTable() error {
	if err := s.CreateTableIfNotExists(TablePushed); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(TablePushed, "language"); err != nil {
		return err
	}
	return s.CreateIndexIfNotExists(TablePushed, "date")
}

func (s *Session) SaveLanguageCount(
	kind, language string,
	date time.Time,
	count int,
) error {
	// Update total.
	_, err := s.Db().Table(kind).Insert(LanguageDateCount{
		Language: language,
		Date:     date,
		Count:    count,
	}).RunWrite(s.Session)
	if err != nil {
		return err
	}
	// Mark aggregate totals for this language and grand as dirty.
	_, err = s.Db().Table(AggregateTable(kind)).Filter(
		gorethink.Row.Field("language").Eq(language).Or(
			gorethink.Row.Field("language").Eq(GrandTotalField)).And(
			gorethink.Row.Field("start").Le(date)).And(
			gorethink.Row.Field("end").Gt(date))).
		Update(map[string]interface{}{
		"total_dirty": true,
	}).RunWrite(s.Session)
	if err != nil {
		return err
	}
	// Mark aggregate ratios and ranks for everything as dirty.
	_, err = s.Db().Table(AggregateTable(kind)).Filter(
		gorethink.Row.Field("start").Le(date).And(
			gorethink.Row.Field("end").Gt(date))).
		Update(map[string]interface{}{
		"ratio_dirty": true,
		"rank_dirty":  true,
	}).RunWrite(s.Session)
	return err
}

func (s *Session) LastLanguageCount(kind string) (
	ldc LanguageDateCount, found bool, err error) {
	cur, err := s.Db().Table(kind).Group("language").Max("date").Field("date").
		Ungroup().Map(func(row gorethink.Term) interface{} {
		return map[string]interface{}{
			"language": row.Field("group"),
			"date":     row.Field("reduction"),
		}
	}).OrderBy("date").Limit(1).Run(s.Session)
	if err != nil || cur.IsNil() {
		return
	}
	defer cur.Close()
	found = true
	err = cur.One(&ldc)
	return
}

func (s *Session) FirstLanguageCount(kind string) (
	ldc LanguageDateCount, found bool, err error) {
	cur, err := s.Db().Table(kind).Group("language").Min("date").Field("date").
		Ungroup().Map(func(row gorethink.Term) interface{} {
		return map[string]interface{}{
			"language": row.Field("group"),
			"date":     row.Field("reduction"),
		}
	}).OrderBy(gorethink.Desc("date")).Limit(1).Run(s.Session)
	if err != nil || cur.IsNil() {
		return
	}
	defer cur.Close()
	found = true
	err = cur.One(&ldc)
	return
}

func (s *Session) EarliestCounts(table string) ([]LanguageDateCount, error) {
	cur, err := s.Db().Table(table).Group("language").Min("date").
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
	cur, err := s.Db().Table(table).Group("language").Max("date").
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
