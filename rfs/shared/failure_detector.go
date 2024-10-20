package shared

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/caleberi/distributed-system/rfs/common"
	"github.com/caleberi/distributed-system/rfs/utils"
	"github.com/go-redis/redis"
	"github.com/rs/zerolog/log"
)

const storePrefix = "__store__"

// A failure detection package - using ϕ Accrual Failure Detection Algorithm
// Sources - (1) https://medium.com/@arpitbhayani/phi-%CF%86-accrual-failure-detection-79c21ce53a7a
//           (2) https://ieeexplore.ieee.org/abstract/document/1353004
//
// Goal - create redis backed service that can predict downtime probablity of servers
// The implementation of failure detectors on the receiving server can be decomposed into three basic parts
// as follows:
// 1. Monitoring. The failure detector gathers information from other processes,
//    usually through the network,such as heartbeat arrivals or query-response delays.
//    Since we are leaving the user to provide the necessary data for the system after obtaining
//     it via RPC or REST e.t.c Then we define an NetworkData struct that takes in few information about
//     the network
// 2. Interpretation. Monitoring information is used and interpreted, for instance to decide that a process should be suspected.
// 3. Action. Actions are executed as a response to triggered suspicions. This is normally done within applications.

type TripInfo struct {
	SentAt     time.Time
	RecievedAt time.Time
}

// Valid checks if the interval between clock time on server
// do not tell lies (Since even clocks do lie)
func (tf TripInfo) Valid() bool {
	return tf.SentAt.Before(tf.RecievedAt)
}

type NetworkData struct {
	// Total time taken for a round rountrip (R)
	RoundRobinTrip time.Duration
	// Forward trip detail stores the information about the inital time of
	// request departure to destination server
	ForwardTrip TripInfo
	// Backward trip detail stores the information about the inital time of
	// request departure to destination server
	BackwardTrip TripInfo
	// Server that initiated the network request
	IpAddress common.ServerAddr
}

// Suspicion level between a scale of 100
type SuspicionLevel struct {
	AccruementThreshold, UpperBoundThreshold, ResetThreshold float64
}

type ActionMessage string

const (
	accumentThresholdAlert   ActionMessage = "<ALERT> TENDING TOWARDS SYSTEM FAILURE"
	upperBoundThresholdAlert ActionMessage = "<WARNING> SYSTEM STILL CORRECT"
	resetThresholdAlert      ActionMessage = "<INFO> SYSTEM ALL GOOD"

	ErrorHistoricalSampling         string = "<ERROR> HISTORICAL SAMPLING ISSUES"
	ErrorNotEnoughistoricalSampling string = "<ERROR> NO ENOUGH HISTORICAL SAMPLES"
)

type Prediction struct {
	// The resulting ϕ value
	Phil float64
	// Action message
	Message ActionMessage
}

type FailureDetector struct {
	// - Keeping track of historical networkdata for
	// failure detection
	Cache *redis.Client
	// number of time to clean up history during the  clean timeInterval
	RefreshRate int64
	// historical data expiration duration
	CleanUpInterval time.Duration
	ShutdownCh      chan bool

	SuspicionLevel SuspicionLevel
}

func NewFailureDetector(
	url string,
	refreshRate int64,
	cleanUpInterval time.Duration,
	suspicionLevel SuspicionLevel) (*FailureDetector, error) {
	cache := redis.NewClient(&redis.Options{Addr: url})

	if _, err := cache.Ping().Result(); err != nil {
		return nil, err
	}

	failureDetector := &FailureDetector{
		Cache:           cache,
		RefreshRate:     refreshRate,
		CleanUpInterval: cleanUpInterval,
		ShutdownCh:      make(chan bool, 1),
		SuspicionLevel:  suspicionLevel,
	}

	flushingTicker := time.NewTicker(failureDetector.CleanUpInterval / time.Duration(refreshRate))
	go func() {
		for {
			select {
			case <-flushingTicker.C: // try to get all prefixed key with the __eta_store
				keys, err := failureDetector.Cache.Keys(storePrefix + "*").Result()
				if err != nil {
					log.Err(err).Msg("")
				}

				utils.ForEach(keys, func(k string) {
					fields, err := failureDetector.Cache.HKeys(k).Result()
					if err != nil {
						log.Err(err).Msg("")
					}
					failureDetector.Cache.HDel(k, fields...)
				})

			case <-failureDetector.ShutdownCh:
				return
			}
		}
	}()
	return failureDetector, nil
}

type Points struct {
	Data []float64 `json:"data,omitempty"`
}

func (fd *FailureDetector) Store(data NetworkData) error {
	key := storePrefix + string(data.IpAddress)
	ttls := Points{}
	result, err := fd.Cache.HGet(key, "*").Result()
	if err != nil && err != redis.Nil {
		log.Err(err).Stack().Msg("")
		return err
	}
	if result != "" {
		err = json.Unmarshal([]byte(result), &ttls)
		if err != nil {
			return err
		}
	}

	utils.ForEach(ttls.Data, func(v float64) { ttls.Data = append(ttls.Data, v) })

	if !data.ForwardTrip.Valid() && !data.BackwardTrip.Valid() {
		return fmt.Errorf(
			"the time %v and %v for the provided forward and backward is invalid",
			data.ForwardTrip,
			data.BackwardTrip)
	}
	if data.RoundRobinTrip == 0 {
		timeToForwardDestination := data.ForwardTrip.RecievedAt.Sub(data.ForwardTrip.SentAt).Milliseconds()
		timeToBackwardDestination := data.BackwardTrip.RecievedAt.Sub(data.BackwardTrip.SentAt).Milliseconds()
		data.RoundRobinTrip = time.Duration(timeToBackwardDestination + timeToForwardDestination)
	}

	log.Info().Msgf("NetworkData = %#v\n", data)
	log.Info().Msgf("RoundRobinTrip = %v\n", data.RoundRobinTrip)
	ttls.Data = append(ttls.Data, float64(data.RoundRobinTrip))
	jsn, err := json.Marshal(ttls)
	if err != nil {
		return err
	}
	return fd.Cache.HSet(key, "*", jsn).Err()
}

func calculatePhil(x, mean, stdDev float64) float64 {
	y := (x - mean) / stdDev
	// https://github.com/akka/akka/issues/1821
	e := math.Exp(-y / (1.5976 + 0.070566*y*y))

	if x > mean {
		return -math.Log(e / (1.0 + e))
	}
	return -math.Log10(1.0 - 1.0/(1.0+e))
}

func (fd *FailureDetector) Predict(addr string, windowSize int) (Prediction, error) {
	// - First, heartbeats arrive and their arrival times are stored in a sampling window.
	window, err := fd.sample(addr, windowSize)
	log.Info().Msgf("window = %v\n", window)
	if err != nil {
		return Prediction{}, errors.New(ErrorHistoricalSampling)
	}

	if len(window) < 2 {
		return Prediction{}, errors.New(ErrorNotEnoughistoricalSampling)
	}
	// - Second,these past samples are used to determine the distributionof inter-arrival times.
	mean := utils.Sum(window) / float64(len(window))
	totalSquaredInterval := 0.0
	utils.ForEach(window, func(v float64) { totalSquaredInterval += math.Pow(v-mean, 2.0) })
	variance := totalSquaredInterval / float64(len(window))
	log.Info().Msgf("variance = %v\n", variance)
	stdDev := math.Sqrt(variance)
	log.Info().Msgf("stdDev = %v\n", stdDev)
	tNow := float64(time.Since(time.Time{}))
	tLast := window[len(window)-1]
	// - Third, the distribution is in turn used to compute the current value of ϕ.
	phil := calculatePhil(tNow-tLast, mean, stdDev) * 100

	if phil == math.Inf(-1) {
		return Prediction{}, nil
	}
	log.Info().Msgf("phil = %v\n", phil)
	action := resetThresholdAlert

	if phil >= fd.SuspicionLevel.AccruementThreshold {
		action = accumentThresholdAlert
	}

	if phil >= fd.SuspicionLevel.UpperBoundThreshold && phil < fd.SuspicionLevel.AccruementThreshold {
		action = upperBoundThresholdAlert
	}

	return Prediction{
		Phil:    phil,
		Message: action,
	}, nil
}

func (fd *FailureDetector) sample(addr string, windowSize int) ([]float64, error) {
	key := storePrefix + addr
	ttls := Points{}

	result, err := fd.Cache.HGet(key, "*").Result()
	if err != nil && err != redis.Nil {
		log.Err(err).Stack().Msg("")
		return nil, err
	}

	if result != "" {
		err = json.Unmarshal([]byte(result), &ttls)
		if err != nil {
			return nil, err
		}
	}

	utils.ForEach(ttls.Data, func(v float64) { ttls.Data = append(ttls.Data, v) })

	var sampleSpace []float64

	for i, j := windowSize-1, len(ttls.Data)-1; windowSize > 0 && j > 0; {
		sampleSpace = append(sampleSpace, ttls.Data[j])
		j -= 1
		i -= 1
	}

	return sampleSpace, nil
}
