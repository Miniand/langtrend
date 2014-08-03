package db

import (
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
		keys := row.Keys().Filter(func(key gorethink.Term) interface{} {
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

func (s *Session) CreateLanguage(lang string) error {
	_, err := s.Db().Table("languages").Insert(map[string]interface{}{
		"id": lang,
	}).RunWrite(s.Session)
	return err
}

func (s *Session) SaveLanguageCount(lang string, date time.Time, count int) error {
	_, err := s.Db().Table("languages").Update(map[string]interface{}{
		"id":                lang,
		github.Format(date): count,
	}).RunWrite(s.Session)
	return err
}
