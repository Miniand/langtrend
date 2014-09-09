package db

import (
	"time"

	"github.com/dancannon/gorethink"
	"github.com/dancannon/gorethink/encoding"
)

const (
	TableJobs = "jobs"
)

type Job struct {
	Id        string      `gorethink:"id,omitempty"`
	Type      string      `gorethink:"type"`
	Args      interface{} `gorethink:"args"`
	CreatedAt time.Time   `gorethink:"createdAt"`
	After     time.Time   `gorethink:"after"`
}

func (s *Session) Enqueue(j Job) error {
	j.CreatedAt = time.Now()
	if j.After.IsZero() {
		j.After = time.Now()
	}
	_, err := s.Db().Table(TableJobs).Insert(j, gorethink.InsertOpts{
		Conflict: "update",
	}).RunWrite(s.Session)
	return err
}

func (s *Session) NextJob() (job Job, ok bool, err error) {
	wr, err := s.Db().Table(TableJobs).Filter(
		gorethink.Not(gorethink.Row.HasFields("after")).Or(
			gorethink.Row.Field("after").Le(time.Now()))).
		OrderBy("after", "createdAt").Limit(1).Delete(gorethink.DeleteOpts{
		ReturnChanges: true,
	}).RunWrite(s.Session)
	if err != nil {
		return
	}
	if wr.Changes == nil || len(wr.Changes) == 0 {
		ok = false
		return
	}
	if err = encoding.Decode(&job, wr.Changes[0].OldValue); err != nil {
		return
	}
	ok = true
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
	return nil
}
