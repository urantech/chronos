package cron

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
	args := m.Called(ctx, name)
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
	mockArgs := h.Called(ctx, args, results)
	return mockArgs.Error(0)
}

func TestStartJob_Success(t *testing.T) {
	// Arrange
	jobName := "test-job"
	mockStorage := new(MockStorage)
	mockHandler := new(MockHandler)

	mockStorage.On("Lock", mock.Anything, jobName).Return(nil)
	mockStorage.On("Unlock", mock.Anything, jobName).Return(nil)
	mockHandler.On("RunCron", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	engine := NewEngine(mockStorage)
	engine.Register(jobName, mockHandler)

	// Act
	errs, err := engine.StartJob(context.Background(), jobName)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 0, len(errs))
}
