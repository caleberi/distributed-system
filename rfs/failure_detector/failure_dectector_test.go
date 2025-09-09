package failuredetector

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestDetector(t *testing.T, windowSize int, entryExpiryTime time.Duration) (*FailureDetector, *miniredis.Miniredis) {
	mr := miniredis.RunT(t)
	detector, err := NewFailureDetector(
		"test-server",
		windowSize,
		&redis.Options{Addr: mr.Addr()},
		entryExpiryTime,
		SuspicionLevel{
			AccruementThreshold: 8.0,
			UpperBoundThreshold: 1.0,
		},
	)
	require.NoError(t, err)
	return detector, mr
}

func TestNewFailureDetector(t *testing.T) {
	detector, _ := setupTestDetector(t, 10, time.Second)
	assert.NotNil(t, detector)
	assert.NotNil(t, detector.Window)
	assert.NotNil(t, detector.ShutdownCh)
	assert.Equal(t, 8.0, detector.SuspicionLevel.AccruementThreshold)
}

func TestRecordSample_InvalidData(t *testing.T) {
	detector, _ := setupTestDetector(t, 10, time.Second)
	err := detector.RecordSample(NetworkData{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid trip timestamps")
}

func TestRecordSample_Valid(t *testing.T) {
	detector, _ := setupTestDetector(t, 10, time.Second)
	now := time.Now()
	data := NetworkData{
		ForwardTrip:  TripInfo{SentAt: now.Add(-100 * time.Millisecond), ReceivedAt: now.Add(-50 * time.Millisecond)},
		BackwardTrip: TripInfo{SentAt: now.Add(-50 * time.Millisecond), ReceivedAt: now},
	}
	err := detector.RecordSample(data)
	assert.NoError(t, err)
}

func approx(val float64) float64 {
	return val
}

func TestPhiFunction(t *testing.T) {
	tests := []struct {
		timeDiff    float64
		mean        float64
		stdDev      float64
		expectedPhi float64
	}{
		{1000, 1000, 100, 0.30},         // On mean, low phi
		{1200, 1000, 100, approx(1.64)}, // Slightly above
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("timeDiff=%f", tt.timeDiff), func(t *testing.T) {
			phiVal := phi(tt.timeDiff, tt.mean, tt.stdDev)
			assert.InDelta(t, tt.expectedPhi, phiVal, 0.01)
		})
	}
}

func TestPredictFailure_NotEnoughSamples(t *testing.T) {
	detector, _ := setupTestDetector(t, 10, time.Second)
	_, err := detector.PredictFailure()
	assert.Error(t, err)
	assert.Equal(t, ErrorNotEnoughHistoricalSampling, err.Error())
}

func TestPredictFailure_ZeroVariance(t *testing.T) {
	detector, _ := setupTestDetector(t, 4, time.Minute)
	ctx := context.Background()
	baseTime := time.Now()
	for i := 0; i < 4; i++ {
		entry := Entry{
			Id:       uuid.New().String(),
			Eta:      baseTime.Add(time.Duration(i) * time.Second),
			Duration: time.Second,
		}
		err := detector.Window.Add(ctx, entry)
		assert.NoError(t, err)
	}
	_, err := detector.PredictFailure()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "zero variance")
}

func TestPredictFailure_Healthy(t *testing.T) {
	detector, _ := setupTestDetector(t, 10, time.Minute)
	for i := 0; i < 10; i++ {
		now := time.Now()
		data := NetworkData{
			ForwardTrip:  TripInfo{SentAt: now.Add(-50 * time.Millisecond), ReceivedAt: now.Add(-25 * time.Millisecond)},
			BackwardTrip: TripInfo{SentAt: now.Add(-25 * time.Millisecond), ReceivedAt: now},
		}
		err := detector.RecordSample(data)
		assert.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
	}
	pred, err := detector.PredictFailure()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, pred.Phi, 1.0)
	assert.Equal(t, UpperBoundThresholdAlert, pred.Message)
}

func TestPredictFailure_WarningAndAlert(t *testing.T) {
	detector, _ := setupTestDetector(t, 10, time.Minute)
	for i := 0; i < 10; i++ {
		now := time.Now()
		data := NetworkData{
			ForwardTrip:  TripInfo{SentAt: now.Add(-50 * time.Millisecond), ReceivedAt: now.Add(-25 * time.Millisecond)},
			BackwardTrip: TripInfo{SentAt: now.Add(-25 * time.Millisecond), ReceivedAt: now},
		}
		err := detector.RecordSample(data)
		assert.NoError(t, err)
		time.Sleep(10 * time.Millisecond) // Regular intervals ~100ms
	}

	pred, err := detector.PredictFailure()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, pred.Phi, 1.0)
	assert.Equal(t, UpperBoundThresholdAlert, pred.Message)

	time.Sleep(1 * time.Second)
	pred, err = detector.PredictFailure()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, pred.Phi, 8.0)
	assert.Equal(t, AccumentThresholdAlert, pred.Message)
}

func TestSamplingWindow_AddGetClean(t *testing.T) {
	mr := miniredis.RunT(t)
	opts := &redis.Options{Addr: mr.Addr()}
	window, err := NewSamplingWindow[Entry]("test-window", 3, 100*time.Millisecond, opts)
	assert.NoError(t, err)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		entry := Entry{
			Id:       fmt.Sprintf("id-%d", i),
			Eta:      time.Now(),
			Duration: time.Duration(i) * time.Second,
		}
		err := window.Add(ctx, entry)
		assert.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
	}

	entries, err := window.Get(ctx)
	assert.NoError(t, err)
	assert.Len(t, entries, 3)

	time.Sleep(200 * time.Millisecond)
	err = window.clean(ctx)
	assert.NoError(t, err)
	entries, err = window.Get(ctx)
	assert.NoError(t, err)
	assert.Len(t, entries, 0)
}

func TestSuspicionLevel_Interpret(t *testing.T) {
	sl := SuspicionLevel{AccruementThreshold: 8.0, UpperBoundThreshold: 1.0}
	tests := []struct {
		phi      float64
		expected ActionMessage
		err      bool
	}{
		{10.0, AccumentThresholdAlert, false},
		{5.0, UpperBoundThresholdAlert, false},
		{0.05, ResetThresholdAlert, false},
		{math.NaN(), "", true},
	}
	for _, tt := range tests {
		msg, err := sl.Interpret(tt.phi)
		if tt.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, msg)
		}
	}
}
