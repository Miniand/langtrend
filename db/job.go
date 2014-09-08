package db

import (
	"log"
	"time"

	"github.com/dancannon/gorethink"
	"github.com/dancannon/gorethink/encoding"
)

const (
	TableJobs = "jobs"
)

type Job struct {
	Id    string      `gorethink:"id,omitempty"`
	Type  string      `gorethink:"type"`
	Args  interface{} `gorethink:"args"`
	After time.Time   `gorethink:"after,omitempty"`
}

func (s *Session) Enqueue(j Job) error {
	log.Printf("enqueuing #v", j)
	_, err := s.Db().Table(TableJobs).Insert(j, gorethink.InsertOpts{
		Conflict: "update",
	}).RunWrite(s.Session)
	return err
}

func (s *Session) NextJob() (job Job, ok bool, err error) {
	wr, err := s.Db().Table(TableJobs).Filter(
		gorethink.Row.Field("after").Eq(nil).Or(
			gorethink.Row.Field("after").Le(time.Now()))).
		Limit(1).Delete(gorethink.DeleteOpts{
		ReturnChanges: true,
	}).RunWrite(s.Session)
	if err != nil {
		return
	}
	if wr.Deleted == 0 {
		ok = false
		return
	}
	log.Printf("Fetched job %#v", wr)
	err = encoding.Decode(&job, wr.OldValue)
	return
}

func (s *Session) WaitingJobCount() (count int, err error) {
	cur, err := s.Db().Table(TableJobs).Count().Run(s.Session)
	if err != nil {
		return
	}
	err = cur.One(&count)
	return
}

func (s *Session) CreateJobsTable() error {
	if err := s.CreateTableIfNotExists(TableJobs); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(TableJobs, "type"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(TableJobs, "after"); err != nil {
		return err
	}
	return nil
}
