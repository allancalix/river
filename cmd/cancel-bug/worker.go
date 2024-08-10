package main

import (
	"context"
	"errors"
	"time"

	"github.com/riverqueue/river"
)

type BuggyJobArgs struct {
	TimeoutPeriod time.Duration
	RequiredFails int
}

func (BuggyJobArgs) Kind() string {
	return "BuggyJobArgs"
}

func (args BuggyJobArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 6}
}

type BuggyJobWorker struct {
	river.WorkerDefaults[BuggyJobArgs]
}

func (BuggyJobWorker) Work(ctx context.Context, job *river.Job[BuggyJobArgs]) error {
	if job.Attempt-job.Args.RequiredFails < 1 {
		return errors.New("forced failure")
	}

	time.Sleep(job.Args.TimeoutPeriod)

	return nil
}
