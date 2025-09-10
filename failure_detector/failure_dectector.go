package failuredetector

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/caleberi/distributed-system/utils"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
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

type ActionMessage string

const (
	AccumentThresholdAlert   ActionMessage = "<ALERT> TENDING TOWARDS SYSTEM FAILURE"
	UpperBoundThresholdAlert ActionMessage = "<WARNING> SYSTEM STILL CORRECT"
	ResetThresholdAlert      ActionMessage = "<INFO> SYSTEM ALL GOOD"

	ErrorHistoricalSampling          string = "<ERROR> HISTORICAL SAMPLING ISSUES"
	ErrorNotEnoughHistoricalSampling string = "<ERROR> NO ENOUGH HISTORICAL SAMPLES"
)

type Prediction struct {
	// The resulting ϕ value
	Phi float64 `json:"phi"`
	// Action message
	Message ActionMessage `json:"message"`
}

// TripInfo stores timing information for a single direction trip
type TripInfo struct {
	SentAt     time.Time
	ReceivedAt time.Time
}

// Valid checks if the interval between clock time on server
// do not tell lies (Since even clocks do lie)
// (Deployment must be in the same region due to timezone differences of some % accuracy)
func (tf TripInfo) Valid() bool {
	return !tf.SentAt.IsZero() && !tf.ReceivedAt.IsZero() && tf.SentAt.Before(tf.ReceivedAt)
}

// Latency calculates the duration of a single direction trip
func (tf TripInfo) Latency() time.Duration {
	if !tf.Valid() {
		return 0
	}
	return tf.ReceivedAt.Sub(tf.SentAt)
}

// NetworkData contains monitoring details about a single round trip between nodes
type NetworkData struct {
	RoundTrip    time.Duration // Total round-trip time
	ForwardTrip  TripInfo      // Outbound trip timing
	BackwardTrip TripInfo      // Inbound trip timing
}

// Valid checks if the complete round trip data is consistent
func (nd NetworkData) Valid() bool {
	return nd.ForwardTrip.Valid() && nd.BackwardTrip.Valid()
}

// CalculateRoundTrip computes the total round-trip time based on the formula
// (Et1 - St1) + (Et2 - St2)
func (nd *NetworkData) CalculateRoundTrip() {
	if nd.Valid() {
		nd.RoundTrip = nd.ForwardTrip.Latency() + nd.BackwardTrip.Latency()
	} else {
		nd.RoundTrip = 0
	}
}

type Entry struct {
	Id       string        `json:"id"`
	Eta      time.Time     `json:"when"`
	Duration time.Duration `json:"duration"`
}

func (e Entry) ID() string {
	return e.Id
}

func (e Entry) Score() int64 {
	return e.Eta.UnixMilli()
}

// Suspicion level between a scale of 100
// SuspicionLevel defines threshold values to interpret ϕ values.
// Thresholds are in the range of 0 to 100 (percentage scale).
type SuspicionLevel struct {
	AccruementThreshold float64 // High threshold (ϕ > AccruementThreshold ⇒ Alert)
	UpperBoundThreshold float64 // Mid threshold  (ϕ between AccruementThreshold and UpperBound ⇒ Warning)
}

func (s SuspicionLevel) Interpret(phi float64) (ActionMessage, error) {
	if math.IsNaN(phi) {
		return "", errors.New("phi is not a number")
	}
	log.Printf("phi: %v", phi)
	switch {
	case phi >= s.AccruementThreshold:
		return AccumentThresholdAlert, nil
	case phi >= s.UpperBoundThreshold:
		return UpperBoundThresholdAlert, nil
	default:
		return ResetThresholdAlert, nil
	}
}

// FailureDetector implements a Redis-backed ϕ Accrual Failure Detection service.
// It continuously monitors network latencies (or heartbeat delays) and interprets
// their statistical behavior to estimate the probability of a server being down.
//
// The ϕ value is computed using historical samples stored in Redis, and interpreted
// using configured suspicion thresholds to classify server health.
//
// Fields:
//   - Window: Redis backed sampling window storing recent network latency samples.
//   - ShutdownCh: Channel used to signal shutdown of background cleanup processes.
//   - SuspicionLevel: Defines threshold values used to interpret the computed ϕ score.
type FailureDetector struct {
	ShutdownCh     chan bool
	SuspicionLevel SuspicionLevel
	Window         *SamplingWindow[Entry]
}

// NewFailureDetector initializes a FailureDetector instance using the given parameters.
// It sets up a Redis client, validates connectivity, and starts a background
// cleaner that periodically flushes stale network data.
//
// Parameters:
//   - serverIp: IP address of the current server (used for namespacing keys).
//   - windowSize: Number of recent samples to retain in the sampling window.
//   - redisOpts: Redis connection options.
//   - entryExpiryTime: Duration after which individual samples expire.
//   - suspicionLevel: Thresholds for interpreting ϕ values.
//
// Returns:
//   - Pointer to the initialized FailureDetector.
//   - An error if Redis connection fails.
func NewFailureDetector(
	serverIP string, windowSize int,
	redisOpts *redis.Options,
	entryExpiryTime time.Duration, suspicionLevel SuspicionLevel,
) (*FailureDetector, error) {
	window, err := NewSamplingWindow[Entry](
		serverIP, windowSize,
		entryExpiryTime,
		redisOpts)
	if err != nil {
		return nil, err
	}
	detector := &FailureDetector{
		Window:         window,
		ShutdownCh:     make(chan bool, 1),
		SuspicionLevel: suspicionLevel,
	}
	return detector, nil
}

// RecordSample adds a new network latency sample into the detector's history store in Redis.
func (fd *FailureDetector) RecordSample(data NetworkData) error {
	if !data.Valid() {
		return fmt.Errorf("invalid trip timestamps: forward=%v backward=%v", data.ForwardTrip, data.BackwardTrip)
	}

	data.CalculateRoundTrip()
	heartbeat := Entry{
		Id:       uuid.New().String(),
		Eta:      time.Now(),
		Duration: data.RoundTrip,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return fd.Window.Add(ctx, heartbeat)
}

// phi calculates the suspicion level (φ) for a node based on the time difference
// since the last heartbeat, as defined in the Φ Accrual Failure Detector paper
// (Hayashibara et al., 2004). It assumes heartbeat inter-arrival times follow a
// normal distribution with given mean and standard deviation.
//
// Parameters:
//   - timeDiff: Time difference (in milliseconds) since the last heartbeat (t_now - t_last).
//   - mean: Mean inter-arrival time of heartbeats (in milliseconds).
//   - stdDeviation: Standard deviation of inter-arrival times (in milliseconds).
//
// Returns:
//   - The φ value (suspicion level). Higher values indicate a higher probability of failure.
//   - If stdDeviation is zero, φ cannot be computed, and Inf is returned.
func phi(timeDiff, mean, stdDeviation float64) float64 {
	if stdDeviation == 0 {
		return math.Inf(1) // Cannot compute φ with zero variance
	}

	// Standardize the time difference: z = (t - μ) / σ
	z := (timeDiff - mean) / stdDeviation

	// Compute the CDF of the standard normal distribution: F(z) = (1 + erf(z/√2))/2
	// erf is the error function, available in math package
	cdf := 0.5 * (1 + math.Erf(z/math.Sqrt2))

	// Compute φ = -log10(1 - F(z))
	// If cdf ≈ 1, 1-cdf is very small, so handle edge case to avoid log(0)
	if cdf >= 1.0 {
		return math.Inf(1) // Extremely high suspicion (failure almost certain)
	}
	phiValue := -math.Log10(1.0 - cdf)

	if math.IsNaN(phiValue) || math.IsInf(phiValue, -1) {
		return math.Inf(1) // Handle numerical instability
	}
	return phiValue
}

func (fd *FailureDetector) PredictFailure() (Prediction, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	entries, err := fd.Window.Get(ctx)
	if err != nil {
		return Prediction{}, errors.New(ErrorHistoricalSampling)
	}

	if len(entries) < fd.Window.size/2 {
		return Prediction{}, errors.New(ErrorNotEnoughHistoricalSampling)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Eta.Before(entries[j].Eta)
	})

	arrivalTimes := utils.Map(entries, func(entry Entry) float64 {
		return float64(entry.Eta.UnixMilli())
	})

	intervals := make([]float64, len(arrivalTimes)-1)
	for i := 1; i < len(arrivalTimes); i++ {
		intervals[i-1] = arrivalTimes[i] - arrivalTimes[i-1]
	}

	mean := utils.Sum(intervals) / float64(len(intervals))
	variance := 0.0
	utils.ForEach(intervals, func(v float64) {
		variance += math.Pow(v-mean, 2)
	})
	variance /= float64(len(intervals))

	if variance == 0 {
		return Prediction{}, errors.New("zero variance: cannot compute phi")
	}
	stdDev := math.Sqrt(variance)

	// Compute time difference from last heartbeat
	tNow := float64(time.Now().UnixMilli())
	tLast := arrivalTimes[len(arrivalTimes)-1]
	delta := tNow - tLast

	phiValue := phi(delta, mean, stdDev)
	action, err := fd.SuspicionLevel.Interpret(phiValue)
	if err != nil {
		return Prediction{}, err
	}

	return Prediction{
		Phi:     phiValue,
		Message: action,
	}, nil
}

func (fd *FailureDetector) Shutdown() {
	fd.ShutdownCh <- true
	close(fd.ShutdownCh)
	fd.Window.rdb.Close()
	log.Info().Msg("FailureDetector shutdown complete")
}
