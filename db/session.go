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
	Address   string
	Rethinkdb gorethink.ConnectOpts
}

func (o *Options) database() string {
	if o.Database != "" {
		return o.Database
	}
	return "pla"
}

func (o *Options) address() string {
	if o.Address != "" {
		return o.Address
	}
	return "localhost:28015"
}

func (o *Options) rethinkdb() gorethink.ConnectOpts {
	o.Rethinkdb.Address = o.address()
	o.Rethinkdb.Database = o.database()
	return o.Rethinkdb
}

func Connect(opts Options) (*Session, error) {
	sess, err := gorethink.Connect(opts.rethinkdb())
	if err != nil {
		return nil, err
	}
	return &Session{
		Session: sess,
		Options: opts,
	}, nil
}

func (s *Session) Db() gorethink.Term {
	return gorethink.Db(s.Options.database())
}

func (s *Session) Migrate() error {
	// Create database
	if err := s.CreateDatabase(); err != nil {
		return err
	}
	// Create created table
	if err := s.CreateCreatedTable(); err != nil {
		return err
	}
	// Create pushed table
	if err := s.CreatePushedTable(); err != nil {
		return err
	}
	// Create created_aggregate table
	if err := s.CreateCreatedAggregateTable(); err != nil {
		return err
	}
	// Create pushed_aggregate table
	if err := s.CreatePushedAggregateTable(); err != nil {
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
		if dbName == s.Options.database() {
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

func (s *Session) CreateTableIfNotExists(table string) error {
	cur, err := s.Db().TableList().Run(s.Session)
	if err != nil {
		return err
	}
	defer cur.Close()
	tableName := ""
	found := false
	for cur.Next(&tableName) {
		if tableName == table {
			found = true
			break
		}
	}
	if !found {
		log.Printf("creating table %s\n", table)
		_, err := s.Db().TableCreate(table).RunWrite(s.Session)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) CreateIndexIfNotExists(table, index string) error {
	cur, err := s.Db().Table(table).IndexList().Run(s.Session)
	if err != nil {
		return err
	}
	defer cur.Close()
	indexName := ""
	found := false
	for cur.Next(&indexName) {
		if indexName == index {
			found = true
			break
		}
	}
	if !found {
		log.Printf("creating index %s.%s\n", table, index)
		_, err := s.Db().Table(table).IndexCreate(index).RunWrite(s.Session)
		if err != nil {
			return err
		}
	}
	return nil
}
