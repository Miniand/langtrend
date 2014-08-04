package db

import (
	"fmt"
	"log"
	"time"

	"github.com/Miniand/langtrend/github"
	"github.com/dancannon/gorethink"
)

type LanguageSummary struct {
	Id    string `gorethink:"id"`
	Count int    `gorethink:"count"`
	Min   string `gorethink:"min"`
	Max   string `gorethink:"max"`
}

func (s *Session) QueryLanguageSummaries() (*gorethink.Cursor, error) {
	return s.Db().Table("languages").Map(func(row gorethink.Term) interface{} {
		keys := row.Field("created").Keys().Filter(func(key gorethink.Term) interface{} {
			return key.Match(`^\d{4}-\d{2}-\d{2}$`)
		})
		return map[string]gorethink.Term{
			"id":    row.Field("id"),
			"count": keys.Count(),
			"min":   keys.Min().Default(nil),
			"max":   keys.Max().Default(nil),
		}
	}).OrderBy("count").Run(s.Session)
}

func (s *Session) NextFetchLanguage() (ls LanguageSummary, err error) {
	cur, err := s.QueryLanguageSummaries()
	if err != nil {
		return
	}
	err = cur.One(&ls)
	return
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

func (s *Session) AddUpdatedToLanguages() error {
	_, err := s.Db().Table("languages").Filter(func(row gorethink.Term) interface{} {
		return row.HasFields("updated").Not()
	}).Update(map[string]interface{}{
		"updated": map[string]interface{}{},
	}).RunWrite(s.Session)
	return err
}

func (s *Session) SaveLanguageCount(lang string, date time.Time, count int) error {
	_, err := s.Db().Table("languages").Get(lang).
		Update(map[string]interface{}{
		"created": map[string]interface{}{
			github.Format(date): count,
		},
	}).RunWrite(s.Session)
	return err
}
