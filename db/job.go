package db

import (
	"time"

	"github.com/dancannon/gorethink"
	"github.com/dancannon/gorethink/encoding"
)

const (
	TableJobs        = "jobs"
	TableStartedJobs = "started_jobs"
	JobStateRunning  = "running"
	JobStateFailed   = "failed"
)

type Job struct {
	Id        string      `gorethink:"id,omitempty"`
	Type      string      `gorethink:"type"`
	Args      interface{} `gorethink:"args"`
	CreatedAt time.Time   `gorethink:"createdAt"`
	After     time.Time   `gorethink:"after"`
}

type StartedJob struct {
	Id    string `gorethink:"id,omitempty"`
	State string `gorethink:"state"`
	Error string `gorethink:"error"`
	Job   Job    `gorethink:"job"`
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

func (s *Session) NextJob() (job Job, ok bool, startedJobId string, err error) {
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
	startedJobId, err = s.JobRunning(job)
	return
}

func (s *Session) JobRunning(job Job) (startedJobId string, err error) {
	wr, err := s.Db().Table(TableStartedJobs).Insert(StartedJob{
		State: JobStateRunning,
		Job:   job,
	}).RunWrite(s.Session)
	if err != nil {
		return
	}
	startedJobId = wr.GeneratedKeys[0]
	return
}

func (s *Session) JobFailed(startedJobId string, err error) error {
	_, err = s.Db().Table(TableStartedJobs).Get(startedJobId).Update(
		map[string]interface{}{
			"state": JobStateFailed,
			"error": err.Error(),
		}).RunWrite(s.Session)
	return err
}

func (s *Session) JobComplete(startedJobId string) error {
	_, err := s.Db().Table(TableStartedJobs).Get(startedJobId).Delete().
		RunWrite(s.Session)
	return err
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

func (s *Session) CreateStartedJobsTable() error {
	if err := s.CreateTableIfNotExists(TableStartedJobs); err != nil {
		return err
	}
	return nil
}
