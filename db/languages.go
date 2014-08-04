package db

import (
	"fmt"
	"log"
	"time"

	"github.com/Miniand/langtrend/github"
	"github.com/dancannon/gorethink"
)

type LanguageSummary struct {
	Id     string `gorethink:"id"`
	Count  int    `gorethink:"count"`
	Min    string `gorethink:"min"`
	Max    string `gorethink:"max"`
	Offset int    `gorethink:"offset"`
	Pivot  string `gorethink:"pivot"`
}

func (ls LanguageSummary) NextFetchDate() (time.Time, error) {
	date, err := github.ParseDate(ls.Pivot)
	if err != nil {
		return time.Time{}, err
	}
	return date.Add(time.Duration(ls.Offset*24) * time.Hour), nil
}

func (s *Session) LanguageSummaries(kind string, from, until time.Time) (*gorethink.Cursor, error) {
	return s.Db().Table("languages").Map(func(row gorethink.Term) interface{} {
		keys := row.Field(kind).Keys()
		count := keys.Count()
		min := keys.Min().Default(nil)
		max := keys.Max().Default(nil)
		offset := gorethink.Branch(count.Eq(0), 0, gorethink.Branch(
			max.Lt(github.FormatDate(until)), 1, gorethink.Branch(
				min.Gt(github.FormatDate(from)), -1, nil)))
		pivot := gorethink.Branch(count.Eq(0), github.FormatDate(until),
			gorethink.Branch(offset.Eq(1), max,
				gorethink.Branch(offset.Eq(-1), min, nil)))
		return map[string]gorethink.Term{
			"id":     row.Field("id"),
			"count":  count,
			"min":    min,
			"max":    max,
			"offset": offset,
			"pivot":  pivot,
		}
	}).Filter(func(row gorethink.Term) interface{} {
		return row.Field("offset").Ne(nil)
	}).OrderBy("count").Run(s.Session)
}

func (s *Session) NextLanguageToFetch(kind string, from, until time.Time) (ls LanguageSummary, found bool, err error) {
	cur, err := s.LanguageSummaries(kind, from, until)
	if err != nil {
		return
	}
	if cur.Next(&ls) {
		found = true
	}
	return
}

func (s *Session) NextLanguageToFetchCreated(from, until time.Time) (ls LanguageSummary, found bool, err error) {
	return s.NextLanguageToFetch("created", from, until)
}

func (s *Session) NextLanguageToFetchPushed(from, until time.Time) (ls LanguageSummary, found bool, err error) {
	return s.NextLanguageToFetch("pushed", from, until)
}

func (s *Session) LanguageExists(lang string) (bool, error) {
	cur, err := s.Db().Table("languages").Get(lang).Run(s.Session)
	if err != nil {
		return false, err
	}
	return !cur.IsNil(), nil
}

func (s *Session) CreateLanguagesTable() error {
	cur, err := s.Db().TableList().Run(s.Session)
	if err != nil {
		return err
	}
	tableName := ""
	found := false
	for cur.Next(&tableName) {
		if tableName == "languages" {
			found = true
			break
		}
	}
	if !found {
		log.Println("creating table languages")
		_, err := s.Db().TableCreate("languages").RunWrite(s.Session)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) CreateLanguages() error {
	for _, l := range github.Languages {
		exists, err := s.LanguageExists(l)
		if err != nil {
			return fmt.Errorf(
				"unable to check if language %s exists, %s\n", l, err)
		}
		if exists {
			continue
		}
		if err := s.CreateLanguage(l); err != nil {
			return fmt.Errorf(
				"unable to create language row for %s, %s\n", l, err)
		}
	}
	return nil
}

func (s *Session) CreateLanguage(lang string) error {
	_, err := s.Db().Table("languages").Insert(map[string]interface{}{
		"id": lang,
	}).RunWrite(s.Session)
	return err
}

func (s *Session) AddCreatedToLanguages() error {
	_, err := s.Db().Table("languages").Filter(func(row gorethink.Term) interface{} {
		return row.HasFields("created").Not()
	}).Update(map[string]interface{}{
		"created": map[string]interface{}{},
	}).RunWrite(s.Session)
	return err
}

func (s *Session) AddPushedToLanguages() error {
	_, err := s.Db().Table("languages").Filter(func(row gorethink.Term) interface{} {
		return row.HasFields("pushed").Not()
	}).Update(map[string]interface{}{
		"pushed": map[string]interface{}{},
	}).RunWrite(s.Session)
	return err
}

func (s *Session) SaveLanguageCount(kind, lang string, date time.Time, count int) error {
	_, err := s.Db().Table("languages").Get(lang).
		Update(map[string]interface{}{
		kind: map[string]interface{}{
			github.FormatDate(date): count,
		},
	}).RunWrite(s.Session)
	return err
}

func (s *Session) SaveLanguageCreatedCount(lang string, date time.Time, count int) error {
	return s.SaveLanguageCount("created", lang, date, count)
}

func (s *Session) SaveLanguagePushedCount(lang string, date time.Time, count int) error {
	return s.SaveLanguageCount("pushed", lang, date, count)
}
