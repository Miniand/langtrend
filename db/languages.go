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
	table, language string,
	date time.Time,
	count int,
) error {
	_, err := s.Db().Table(table).Insert(LanguageDateCount{
		Language: language,
		Date:     date,
		Count:    count,
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
	found = true
	err = cur.One(&ldc)
	return
}

func (s *Session) LanguageCounts(language, kind string) ([]LanguageDateCount, error) {
	cur, err := s.Db().Table(kind).GetAllByIndex("language", language).OrderBy("date").Run(s.Session)
	if err != nil {
		return nil, err
	}
	counts := []LanguageDateCount{}
	err = cur.All(&counts)
	return counts, err
}

func (s *Session) LanguageCountsByWeek(language, kind string) ([]LanguageDateCount, error) {
	cur, err := s.Db().Table(kind).GetAllByIndex("language", language).Group(
		func(row gorethink.Term) interface{} {
			return row.Field("date").ToEpochTime().Div(604000).CoerceTo("STRING").Split(".").Nth(0).CoerceTo("NUMBER")
		}).Sum("count").Ungroup().Map(func(row gorethink.Term) interface{} {
		return map[string]interface{}{
			"language": language,
			"date":     gorethink.EpochTime(row.Field("group").Mul(604000)),
			"count":    row.Field("reduction"),
		}
	}).OrderBy("date").Run(s.Session)
	if err != nil {
		return nil, err
	}
	counts := []LanguageDateCount{}
	err = cur.All(&counts)
	return counts, err
}
