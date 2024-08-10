package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

func run() error {
	ctx := context.Background()
	db, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/postgres")
	if err != nil {
		return fmt.Errorf("db connect: %w", err)
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, &BuggyJobWorker{})

	rc, err := river.NewClient(riverpgxv5.New(db), &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		Workers: workers,
	})
	if err != nil {
		return fmt.Errorf("river init: %w", err)
	}

	go func() {
		ticker := time.NewTicker(time.Millisecond * 250)
		defer ticker.Stop()

		for range ticker.C {
			jobs, err := rc.JobList(ctx, river.NewJobListParams())
			if err != nil {
				panic(err)
			}

			for _, job := range jobs.Jobs {
				if job.State != rivertype.JobStateRetryable {
					continue
				}

				slog.InfoContext(ctx, "Cancelling job.", "job_id", job.ID, "job_state", job.State)
				_, err = rc.JobCancel(ctx, job.ID)
				if err != nil {
					panic(err)
				}

				// After a successful cancel, wait a bit before cancelling again.
				time.Sleep(time.Second * 30)
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(time.Second * 15)
		defer ticker.Stop()

		ctx := context.Background()
		for range ticker.C {
			_, err = rc.Insert(ctx, BuggyJobArgs{
				TimeoutPeriod: time.Second * 10,
				RequiredFails: 3,
			}, nil)
		}
		if err != nil {
			panic(err)
		}
	}()

	err = rc.Start(ctx)
	if err != nil {
		return fmt.Errorf("river start: %w", err)
	}

	ch, cancel := rc.Subscribe(river.EventKindJobCancelled)
	if err != nil {
		return fmt.Errorf("river subscribe: %w", err)
	}
	go func() {
		defer cancel()

		for event := range ch {
			fmt.Println(event)
			fmt.Println(event.Job.FinalizedAt)
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs

	err = rc.Stop(ctx)
	if err != nil {
		return fmt.Errorf("river stop: %w", err)
	}

	return nil
}

func main() {
	err := run()
	if err != nil {
		panic(err)
	}
}
