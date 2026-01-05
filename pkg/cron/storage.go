package cron

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Storage interface {
	Lock(ctx context.Context, name string) error
	Unlock(ctx context.Context, name string) error
	SaveCursor(ctx context.Context, name string, ts *timestamppb.Timestamp) error
	GetCursor(ctx context.Context, name string) (*timestamppb.Timestamp, error)
}

type RedisStorage struct {
	client *redis.Client
}

func NewRedisStorage(client *redis.Client) *RedisStorage {
	return &RedisStorage{client: client}
}

func (s *RedisStorage) Lock(ctx context.Context, name string) error {
	lockKey := "lock:" + name

	success, err := s.client.SetNX(ctx, lockKey, "processing", time.Hour).Result()
	if err != nil {
		return fmt.Errorf("error while set lock if exists: %w", err)
	}

	if !success {
		return fmt.Errorf("error while trying lock: %w", err)
	}

	return nil
}

func (s *RedisStorage) Unlock(ctx context.Context, name string) error {
	return s.client.Del(ctx, "lock:"+name).Err()
}

func (s *RedisStorage) SaveCursor(ctx context.Context, name string, ts *timestamppb.Timestamp) error {
	cursorKey := "cursor:" + name
	t := ts.AsTime().Format(time.RFC3339)
	return s.client.Set(ctx, cursorKey, t, 0).Err()
}

func (s *RedisStorage) GetCursor(ctx context.Context, name string) (*timestamppb.Timestamp, error) {
	cursorKey := "cursor:" + name

	val, err := s.client.Get(ctx, cursorKey).Result()
	if err != nil {
		return nil, nil
	}

	t, err := time.Parse(time.RFC3339, val)
	if err != nil {
		return nil, fmt.Errorf("error parse time: %w", err)
	}

	return timestamppb.New(t), nil
}
