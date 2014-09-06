package db

import (
	"log"
	"strings"

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
	if err := s.CreateCountTable(TableCreated); err != nil {
		return err
	}
	// Create pushed table
	if err := s.CreateCountTable(TablePushed); err != nil {
		return err
	}
	// Create created_aggregate table
	if err := s.CreateAggregateTable(TableCreated); err != nil {
		return err
	}
	// Create pushed_aggregate table
	if err := s.CreateAggregateTable(TablePushed); err != nil {
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
		log.Printf("creating database %s\n", s.Options.database())
		_, err := gorethink.DbCreate(s.Options.database()).RunWrite(s.Session)
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

func IndexName(indexes ...string) string {
	return strings.Join(indexes, ":")
}

func (s *Session) CreateIndexIfNotExists(table string, indexes ...string) error {
	cur, err := s.Db().Table(table).IndexList().Run(s.Session)
	if err != nil {
		return err
	}
	indexName := IndexName(indexes...)
	n := ""
	found := false
	for cur.Next(&n) {
		if n == indexName {
			found = true
			break
		}
	}
	if !found {
		log.Printf("creating index %s.%s\n", table, indexName)
		if len(indexes) == 1 {
			_, err := s.Db().Table(table).IndexCreate(indexes[0]).RunWrite(
				s.Session)
			if err != nil {
				return nil
			}
		} else {
			_, err := s.Db().Table(table).IndexCreateFunc(indexName,
				func(row gorethink.Term) interface{} {
					index := make([]interface{}, len(indexes))
					for i, in := range indexes {
						index[i] = row.Field(in)
					}
					return index
				}).RunWrite(s.Session)
			if err != nil {
				return err
			}
		}
		s.Db().Table(table).IndexWait(indexName).Run(s.Session)
	}
	return nil
}
