package cron

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "chronos/gen/go/cron"
)

type MockStorage struct {
	mock.Mock
}

type MockHandler struct {
	mock.Mock
}

func (m *MockStorage) Lock(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

func (m *MockStorage) Unlock(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

func (m *MockStorage) SaveCursor(ctx context.Context, name string, ts *timestamppb.Timestamp) error {
	args := m.Called(ctx, name, ts)
	return args.Error(0)
}

func (m *MockStorage) GetCursor(ctx context.Context, name string) (*timestamppb.Timestamp, error) {
	args := m.Called(ctx, name)
	if args.Get(0) != nil {
		return args.Get(0).(*timestamppb.Timestamp), nil
	}
	return nil, args.Error(1)
}

func (h *MockHandler) RunCron(ctx context.Context, args *pb.JobArgs, results chan<- *pb.JobProgress) error {
	argsCalled := h.Called(ctx, args, results)
	return argsCalled.Error(0)
}

func TestStartJob_Success(t *testing.T) {
	// Arrange
	jobName := "test-job"
	mockStorage := new(MockStorage)
	mockHandler := new(MockHandler)

	mockStorage.On("Lock", mock.Anything, jobName).Return(nil)
	mockStorage.On("Unlock", mock.Anything, jobName).Return(nil)

	lastCheckpoint := timestamppb.Now()
	mockStorage.On("GetCursor", mock.Anything, jobName).Return(lastCheckpoint, nil)
	mockStorage.On("SaveCursor", mock.Anything, jobName, mock.Anything).Return(nil)

	mockHandler.On("RunCron", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		results := args.Get(2).(chan<- *pb.JobProgress)

		results <- &pb.JobProgress{
			Status:            pb.JobStatus_JOB_STATUS_SUCCESS,
			CurrentCheckpoint: timestamppb.Now(),
		}
	}).Return(nil)

	engine := NewEngine(mockStorage)
	engine.Register(jobName, mockHandler)

	// Act
	errs, err := engine.StartJob(context.Background(), jobName)

	// Assert
	assert.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	select {
	case err := <-errs:
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	default:
	}

	_, ok := <-errs
	assert.False(t, ok, "error channel should be closed")

	mockStorage.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
}

func TestStartJob_ClientError(t *testing.T) {
	// Arrange
	jobName := "test-job"
	mockStorage := new(MockStorage)
	mockHandler := new(MockHandler)

	mockStorage.On("Lock", mock.Anything, jobName).Return(nil)
	mockStorage.On("Unlock", mock.Anything, jobName).Return(nil)

	lastCheckpoint := timestamppb.Now()
	mockStorage.On("GetCursor", mock.Anything, jobName).Return(lastCheckpoint, nil)

	mockHandler.On("RunCron", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some error"))

	engine := NewEngine(mockStorage)
	engine.Register(jobName, mockHandler)

	// Act
	errs, err := engine.StartJob(context.Background(), jobName)

	// Assert
	firstErr, ok := <-errs

	require.NoError(t, err)
	require.True(t, ok, "Should receive at least one error")
	assert.ErrorContains(t, firstErr, "some error")

	select {
	case secondErr, ok := <-errs:
		if ok {
			t.Errorf("Expected only one error, got second: %v", secondErr)
		}
	default:
	}

	_, ok = <-errs
	assert.False(t, ok, "Channel should be closed after sending error")

	mockStorage.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
}
