package db

import (
	"log"

	"github.com/Miniand/langtrend/github"
	"github.com/dancannon/gorethink"
)

type Session struct {
	Session *gorethink.Session
	Options Options
}

type Options struct {
	Database  string
	Rethinkdb gorethink.ConnectOpts
}

func Connect(opts Options) (*Session, error) {
	sess, err := gorethink.Connect(opts.Rethinkdb)
	if err != nil {
		return nil, err
	}
	return &Session{
		Session: sess,
		Options: opts,
	}, nil
}

func (s *Session) Db() gorethink.Term {
	return gorethink.Db(s.Options.Database)
}

func (s *Session) Migrate() error {
	// Create database
	cur, err := gorethink.DbList().Run(s.Session)
	if err != nil {
		return err
	}
	dbName := ""
	found := false
	for cur.Next(&dbName) {
		if dbName == s.Options.Database {
			found = true
			break
		}
	}
	if !found {
		log.Printf("creating database %s\n", s.Options.Database)
		_, err := gorethink.DbCreate(s.Options.Database).RunWrite(s.Session)
		if err != nil {
			return err
		}
	}
	// Create languages table
	cur, err = s.Db().TableList().Run(s.Session)
	if err != nil {
		return err
	}
	tableName := ""
	found = false
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
	// Ensure row for all languages exists
	for _, l := range github.Languages {
		exists, err := s.LanguageExists(l)
		if err != nil {
			log.Fatalf("unable to check if language %s exists, %s\n", l, err)
		}
		if exists {
			continue
		}
		if err := s.CreateLanguage(l); err != nil {
			log.Fatalf("unable to create language row for %s, %s\n", l, err)
		}
	}
	return nil
}
