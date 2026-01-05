package cron

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"
)

type Storage interface {
	Lock(ctx context.Context, name string) error
	Unlock(ctx context.Context, name string) error
	SaveCursor(ctx context.Context, name string, ts *timestamppb.Timestamp) error
	GetCursor(ctx context.Context, name string) (*timestamppb.Timestamp, error)
}
