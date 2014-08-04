package db

import (
	"time"

	"github.com/Miniand/langtrend/github"
	"github.com/dancannon/gorethink"
)

const (
	TableCreated = "created"
	TablePushed  = "pushed"
)

type LanguageDateCount struct {
	Id       string `gorethink:"id"`
	Language string `gorethink:"language"`
	Date     string `gorethink:"date"`
	Count    int    `gorethink:"count"`
}

func (l LanguageDateCount) Time() (time.Time, error) {
	return github.ParseDate(l.Date)
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
	_, err := s.Db().Table(table).Insert(map[string]interface{}{
		"language": language,
		"date":     github.FormatDate(date),
		"count":    count,
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
