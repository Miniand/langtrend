package db

import (
	"time"

	"github.com/dancannon/gorethink"
	"github.com/dancannon/gorethink/encoding"
)

const (
	TableJobs = "jobs"
)

const (
	JobStateWaiting = iota
	JobStateRunning
	JobStateFailed
)

type Job struct {
	Id        string      `gorethink:"id,omitempty"`
	Type      string      `gorethink:"type"`
	State     int         `gorethink:"state"`
	Args      interface{} `gorethink:"args"`
	CreatedAt time.Time   `gorethink:"createdAt"`
	After     time.Time   `gorethink:"after"`
	Error     string      `gorethink:"error"`
}

func (s *Session) Enqueue(j Job) error {
	j.State = JobStateWaiting
	j.CreatedAt = time.Now()
	if j.After.IsZero() {
		j.After = time.Now()
	}
	_, err := s.Db().Table(TableJobs).Insert(j, gorethink.InsertOpts{
		Durability: "soft",
		Conflict:   "update",
	}).RunWrite(s.Session)
	return err
}

func (s *Session) NextJob() (job Job, ok bool, err error) {
	wr, err := s.Db().Table(TableJobs).OrderBy(gorethink.OrderByOpts{
		Index: IndexName("state", "after", "createdAt"),
	}).Limit(1).Filter(gorethink.Row.Field("state").Eq(JobStateWaiting)).
		Update(map[string]interface{}{
		"state": JobStateRunning,
	}, gorethink.UpdateOpts{
		ReturnChanges: true,
	}).RunWrite(s.Session)
	if err != nil {
		return
	}
	if wr.Changes == nil || len(wr.Changes) == 0 {
		ok = false
		return
	}
	if err = encoding.Decode(&job, wr.Changes[0].NewValue); err != nil {
		return
	}
	ok = true
	return
}

func (s *Session) JobFailed(jobId string, err error) error {
	_, err = s.Db().Table(TableJobs).Get(jobId).Update(
		map[string]interface{}{
			"state": JobStateFailed,
			"error": err.Error(),
		}).RunWrite(s.Session)
	return err
}

func (s *Session) JobComplete(jobId string) error {
	_, err := s.Db().Table(TableJobs).Get(jobId).Delete().
		RunWrite(s.Session)
	return err
}

func (s *Session) WaitingJobCount() (count int, err error) {
	cur, err := s.Db().Table(TableJobs).Count().Run(s.Session)
	if err != nil {
		return
	}
	defer cur.Close()
	err = cur.One(&count)
	return
}

func (s *Session) CreateJobsTable() error {
	if err := s.CreateTableIfNotExists(TableJobs); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(TableJobs, "state"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(TableJobs, "createdAt"); err != nil {
		return err
	}
	if err := s.CreateIndexIfNotExists(TableJobs, "state", "after", "createdAt"); err != nil {
		return err
	}
	return nil
}
