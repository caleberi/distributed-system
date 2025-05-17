package shared

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/caleberi/distributed-system/rfs/utils"
	"github.com/go-redis/redis"
	"github.com/rs/zerolog/log"
)

// A failure detection package - using ϕ Accrual Failure Detection Algorithm
// Sources - (1) https://medium.com/@arpitbhayani/phi-%CF%86-accrual-failure-detection-79c21ce53a7a
//
//	(2) https://ieeexplore.ieee.org/abstract/document/1353004
//
// Goal - create redis backed service that can predict downtime probablity of servers
// The implementation of failure detectors on the receiving server can be decomposed into three basic parts
// as follows:
//  1. Monitoring. The failure detector gathers information from other processes,
//     usually through the network,such as heartbeat arrivals or query-response delays.
//     Since we are leaving the user to provide the necessary data for the system after obtaining
//     it via RPC or REST e.t.c Then we define an NetworkData struct that takes in few information about
//     the network
//  2. Interpretation. Monitoring information is used and interpreted, for instance to decide that a process should be suspected.
//  3. Action. Actions are executed as a response to triggered suspicions. This is normally done within applications.
//
// Note: Even though deployments should ideally be in the same region,
// clock drift is always a concern: - Support for NTP sync or logical clocks
//
//	Optional adjustment using known drift offset if available
type TripInfo struct {
	SentAt     time.Time
	RecievedAt time.Time
}

// Valid checks if the interval between clock time on server
// do not tell lies (Since even clocks do lie)
// (Deployment must be in the same region due to timezone differences of some % accuracy)
func (tf TripInfo) Valid() bool {
	return tf.SentAt.Before(tf.RecievedAt)
}

func (t TripInfo) Latency() time.Duration {
	return t.RecievedAt.Sub(t.SentAt)
}

// NetworkData contains monitoring details about a single round trip between nodes.
type NetworkData struct {
	RoundTrip    time.Duration // Total round-trip time
	ForwardTrip  TripInfo      // Outbound trip timing
	BackwardTrip TripInfo      // Inbound trip timing
}

// Suspicion level between a scale of 100
// SuspicionLevel defines threshold values to interpret ϕ values.
// Thresholds are in the range of 0 to 100 (percentage scale).
type SuspicionLevel struct {
	AccruementThreshold float64 // ϕ > AccruementThreshold ⇒ Alert
	UpperBoundThreshold float64 // ϕ between AccruementThreshold and UpperBound ⇒ Warning
	ResetThreshold      float64 // ϕ < ResetThreshold ⇒ System considered healthy
}

func (s SuspicionLevel) Interpret(phi float64) (ActionMessage, error) {
	switch {
	case phi >= s.AccruementThreshold:
		return accumentThresholdAlert, nil
	case phi >= s.UpperBoundThreshold:
		return upperBoundThresholdAlert, nil
	case phi <= s.ResetThreshold:
		return resetThresholdAlert, nil
	default:
		return "", fmt.Errorf("ϕ %.2f does not fit any known threshold", phi)
	}
}

type ActionMessage string

const (
	accumentThresholdAlert   ActionMessage = "<ALERT> TENDING TOWARDS SYSTEM FAILURE"
	upperBoundThresholdAlert ActionMessage = "<WARNING> SYSTEM STILL CORRECT"
	resetThresholdAlert      ActionMessage = "<INFO> SYSTEM ALL GOOD"

	ErrorHistoricalSampling          string = "<ERROR> HISTORICAL SAMPLING ISSUES"
	ErrorNotEnoughHistoricalSampling string = "<ERROR> NO ENOUGH HISTORICAL SAMPLES"
)

type Prediction struct {
	// The resulting ϕ value
	Phi float64 `json:"phi"`
	// Action message
	Message ActionMessage `json:"message"`
}

// FailureDetector implements a Redis-backed ϕ Accrual Failure Detection service.
// It continuously monitors network latencies (or heartbeat delays) and interprets
// their statistical behavior to estimate the probability of a server being down.
//
// The ϕ value is computed using historical samples stored in Redis, and interpreted
// using configured suspicion thresholds to classify server health.
//
// Fields:
//   - KeyPrefix: Redis key prefix used to namespace historical data per monitored server.
//   - Cache: Redis client used for storing and retrieving historical latency data.
//   - RefreshRate: Defines how often cleanup is triggered within CleanUpInterval.
//     For example, a RefreshRate of 2 means cleanup runs every CleanUpInterval / 2.
//   - CleanUpInterval: Duration after which historical latency data is expired or purged.
//   - ShutdownCh: Channel used to signal shutdown of background cleanup processes.
//   - SuspicionLevel: Defines threshold values used to interpret the computed ϕ score.
type FailureDetector struct {
	KeyPrefix       string
	Cache           *redis.Client
	RefreshRate     int64
	CleanUpInterval time.Duration
	ShutdownCh      chan bool
	SuspicionLevel  SuspicionLevel
}

// NewFailureDetector initializes a FailureDetector instance using the given parameters.
// It sets up a Redis client, validates connectivity, and starts a background
// cleaner that periodically flushes stale network data.
//
// Parameters:
//   - serverIp: IP address of the current server (used for namespacing keys).
//   - url: Redis connection address (host:port).
//   - refreshRate: How many times to run cleanup within each cleanUpInterval.
//   - cleanUpInterval: The duration window after which historical data is considered stale.
//   - suspicionLevel: Thresholds used to interpret ϕ values.
//
// Returns:
//   - Pointer to the initialized FailureDetector.
//   - An error if Redis connection fails.
func NewFailureDetector(
	serverIP string,
	redisAddr string,
	refreshRate int64,
	cleanupInterval time.Duration,
	suspicionLevel SuspicionLevel,
) (*FailureDetector, error) {
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})

	if _, err := redisClient.Ping().Result(); err != nil {
		return nil, fmt.Errorf("unable to connect to Redis: %w", err)
	}

	detector := &FailureDetector{
		KeyPrefix:       fmt.Sprintf("fd:%s:samples", serverIP),
		Cache:           redisClient,
		RefreshRate:     refreshRate,
		CleanUpInterval: cleanupInterval,
		ShutdownCh:      make(chan bool, 1),
		SuspicionLevel:  suspicionLevel,
	}

	startCleanupWorker(detector)
	return detector, nil
}

// Points represents a collection of network latency measurements.
// It stores a series of floating-point values that represent round-trip times
// or other network metrics used by the failure detector to identify potential issues.
// The struct is designed for JSON serialization with omitempty to avoid empty arrays in output.
type Points struct {
	// Data contains the time-series measurements in milliseconds.
	// Each value typically represents a round-trip network latency sample.
	// The array grows as new samples are recorded via RecordSample().
	Data []float64 `json:"data,omitempty"`
}

// RecordSample adds a new network latency sample into the detector's history store in Redis.
func (fd *FailureDetector) RecordSample(data NetworkData) error {
	if !data.ForwardTrip.Valid() || !data.BackwardTrip.Valid() {
		return fmt.Errorf(
			"invalid trip timestamps: forward=%v backward=%v",
			data.ForwardTrip, data.BackwardTrip)
	}

	// Compute round trip if not provided
	if data.RoundTrip == 0 {
		forward := data.ForwardTrip.RecievedAt.Sub(data.ForwardTrip.SentAt)
		backward := data.BackwardTrip.RecievedAt.Sub(data.BackwardTrip.SentAt)
		data.RoundTrip = forward + backward
	}

	key := fd.KeyPrefix

	var samples Points
	existing, err := fd.Cache.HGet(key, "samples").Result()
	if err != nil && err != redis.Nil {
		log.Err(err).Str("key", key).Msg("failed to retrieve existing samples")
		return err
	}
	if existing != "" {
		if err := json.Unmarshal([]byte(existing), &samples); err != nil {
			log.Err(err).Str("key", key).Msg("failed to decode samples")
			return err
		}
	}

	samples.Data = append(samples.Data, float64(data.RoundTrip.Nanoseconds()))

	encoded, err := json.Marshal(samples)
	if err != nil {
		log.Err(err).Str("key", key).Msg("failed to encode updated samples")
		return err
	}

	return fd.Cache.HSet(key, "samples", encoded).Err()
}

// Source-BugFix: http://github.com/akka/akka/issues/1821
// phi calculates the suspicion level (φ) for a node based on time difference.
// This implementation follows the Phi Accrual Failure Detector algorithm.
//
// Parameters:
//   - timeDiff: The time difference in milliseconds since the last heartbeat
//   - mean: The mean (average) time between heartbeats in the historical data
//   - stdDeviation: The standard deviation of the time between heartbeats
//
// Returns:
//   - A double representing the suspicion level. Higher values indicate higher
//     probability that the node has failed.
func phi(timeDiff float64, mean float64, stdDeviation float64) float64 {
	y := (timeDiff - mean) / stdDeviation
	e := math.Exp(-y * (1.5976 + 0.070566*y*y))
	var phiValue float64
	if timeDiff > mean {
		phiValue = -math.Log10(e / (1.0 + e))
	} else {
		phiValue = -math.Log10(1.0 - 1.0/(1.0+e))
	}

	return phiValue
}

func (fd *FailureDetector) PredictFailure(windowSize int) (Prediction, error) {
	window, err := fd.sample(windowSize)
	if err != nil {
		log.Err(err).Msg("sampling failed")
		return Prediction{}, errors.New(ErrorHistoricalSampling)
	}
	log.Debug().Msgf("window = %v", window)

	if len(window) < 2 {
		return Prediction{}, errors.New(ErrorNotEnoughHistoricalSampling)
	}

	mean := utils.Sum(window) / float64(len(window))
	variance := 0.0
	utils.ForEach(window, func(v float64) {
		variance += math.Pow(v-mean, 2)
	})
	variance /= float64(len(window))

	if variance == 0 {
		return Prediction{}, errors.New("zero variance: cannot compute phi")
	}
	stdDev := math.Sqrt(variance)

	// Compute time difference from last heartbeat
	tNow := float64(time.Now().Nanosecond())
	tLast := window[len(window)-1]
	delta := tNow - tLast

	phiValue := phi(delta, mean, stdDev) * 100
	if math.IsInf(phiValue, -1) || math.IsNaN(phiValue) {
		return Prediction{}, nil
	}

	var action ActionMessage
	switch {
	case phiValue >= fd.SuspicionLevel.AccruementThreshold:
		action = accumentThresholdAlert
	case phiValue >= fd.SuspicionLevel.UpperBoundThreshold:
		action = upperBoundThresholdAlert
	default:
		action = resetThresholdAlert
	}

	return Prediction{
		Phi:     phiValue,
		Message: action,
	}, nil
}

func (fd *FailureDetector) sample(windowSize int) ([]float64, error) {
	var points Points

	result, err := fd.Cache.HGet(fd.KeyPrefix, "samples").Result()
	if err != nil && err != redis.Nil {
		log.Err(err).Stack().Msg("failed to retrieve points from Redis")
		return nil, err
	}

	if result != "" {
		if err := json.Unmarshal([]byte(result), &points); err != nil {
			log.Err(err).Msg("failed to unmarshal points from Redis result")
			return nil, err
		}
	}

	dataLen := len(points.Data)
	if dataLen == 0 {
		return nil, nil
	}
	start := int(math.Max(float64(dataLen-windowSize), 0.0))
	sample := make([]float64, 0, windowSize)
	sample = append(sample, points.Data[start:]...)

	return sample, nil
}

// startCleanupWorker launches a background goroutine that periodically deletes
// stale historical data from Redis, based on the detector's configuration.
func startCleanupWorker(detector *FailureDetector) {
	go func() {
		cleanupTicker := time.NewTicker(
			detector.CleanUpInterval / time.Duration(detector.RefreshRate))
		defer cleanupTicker.Stop()

		for {
			select {
			case <-cleanupTicker.C:
				keys, err := detector.Cache.Keys(detector.KeyPrefix + "*").Result()
				if err != nil {
					log.Err(err).Msg("failed to retrieve keys for cleanup")
					continue
				}

				utils.ForEach(keys, func(key string) {
					fields, err := detector.Cache.HKeys(key).Result()
					if err != nil {
						log.Err(err).Str("key", key).Msg("failed to retrieve hash fields")
						return
					}
					if len(fields) == 0 {
						return
					}

					if _, err := detector.Cache.HDel(key, fields...).Result(); err != nil {
						log.Err(err).Str("key", key).Msg("failed to delete hash fields")
					}
				})

			case <-detector.ShutdownCh:
				return
			}
		}
	}()
}
