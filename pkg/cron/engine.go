package cron

import (
	"context"
	"fmt"
	"log"
	"sync"

	pb "chronos/gen/go/cron"
)

type Engine struct {
	storage  Storage
	handlers map[string]CronHandler
	wg       sync.WaitGroup
}

func NewEngine(storage Storage) *Engine {
	return &Engine{
		storage:  storage,
		handlers: make(map[string]CronHandler),
	}
}

func (e *Engine) Register(name string, handler CronHandler) {
	e.handlers[name] = handler
}

func (e *Engine) StartJob(ctx context.Context, name string) (<-chan error, error) {
	handler, ok := e.handlers[name]
	if !ok {
		return nil, fmt.Errorf("job %s not registered", name)
	}

	errs := make(chan error, 1)

	e.wg.Go(func() {
		defer close(errs)

		if err := e.storage.Lock(ctx, name); err != nil {
			log.Printf("failed to lock job %s: %v", name, err)
			return
		}
		defer e.storage.Unlock(ctx, name)

		lastCheckpoint, err := e.storage.GetCursor(ctx, name)
		if err != nil {
			log.Printf("failed to get cursor %s: %v", name, err)
			return
		}

		results := make(chan *pb.JobProgress)

		e.wg.Go(func() {
			defer close(results)

			args := &pb.JobArgs{
				JobName:        name,
				LastCheckpoint: lastCheckpoint,
			}

			if err := handler.RunCron(ctx, args, results); err != nil {
				errs <- fmt.Errorf("handler rpc error: %w", err)
			}
		})

		for progress := range results {
			if progress.GetError() != "" {
				errs <- fmt.Errorf("job logic error: %s", progress.GetError())
				return
			}

			if progress.CurrentCheckpoint != nil {
				if err := e.storage.SaveCursor(ctx, name, progress.CurrentCheckpoint); err != nil {
					log.Printf("failed to save checkpoint %s: %v", name, err)
				}
			}

			if progress.GetStatus() == pb.JobStatus_JOB_STATUS_SUCCESS {
				break
			}
		}
	})

	return errs, nil
}
