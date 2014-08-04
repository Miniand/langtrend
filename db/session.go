package db

import (
	"log"

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
	if err := s.CreateDatabase(); err != nil {
		return err
	}
	// Create languages table
	if err := s.CreateLanguagesTable(); err != nil {
		return err
	}
	// Ensure row for all languages exists
	if err := s.CreateLanguages(); err != nil {
		return err
	}
	// Ensure the created field exists
	if err := s.AddCreatedToLanguages(); err != nil {
		return err
	}
	// Ensure the pushed field exists
	if err := s.AddPushedToLanguages(); err != nil {
		return err
	}
	return nil
}

func (s *Session) CreateDatabase() error {
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
	return nil
}
