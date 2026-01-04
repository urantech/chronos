package cron

import (
	"context"

	pb "chronos/gen/go/cron"
)

type CronHandler interface {
	RunCron(ctx context.Context, args *pb.JobArgs, results chan<- *pb.JobProgress) error
}
