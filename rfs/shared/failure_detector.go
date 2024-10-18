package shared

import (
	"errors"
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
//
//	(2) https://ieeexplore.ieee.org/abstract/document/1353004
//
// Goal - create redis backed module that can predict downtime probablity of servers
// The implementation of failure detectors on the receiving server can be decomposed into three basic parts
// as follows:
// 1. Monitoring. The failure detector gathers information from other processes, usually through the network,such as heartbeat arrivals or query-response delays.
//   - Since we are leaving the user to provide the necessary data for the system after obtaining
//     it via RPC or REST e.t.c Then we define an NetworkData struct that takes in few information about
//     the network

type TripInfo struct {
	SentAt     time.Duration
	RecievedAt time.Duration
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

// 2. Interpretation. Monitoring information is used and interpreted,
// for instance to decide that a process should be suspected.

type SuspicionLevel struct {
	AccruementThreshold, UpperBoundThreshold, ResetThreshold float64
}

// 3. Action. Actions are executed as a response to triggered
// suspicions. This is normally done within applications.
type ActionMessage string

const (
	accumentThresholdAlert   ActionMessage = "<ALERT> TENDING TOWARDS SYSTEM"
	upperBoundThresholdAlert ActionMessage = "<WARNING> SYSTEM STILL CORRECT"
	resetThresholdAlert      ActionMessage = "<INFO> SYSTEM ALL GOOD"

	ErrorHistoricalSampling         string = "<ERROR> HISTORICAL SAMPLING ERROR"
	ErrorNotEnoughistoricalSampling string = "<ERROR> HISTORICAL SAMPLING ERROR"
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

func NewFailureDetector(url string, refreshRate int64, cleanUpInterval time.Duration) *FailureDetector {
	Cache := redis.NewClient(&redis.Options{Addr: url})

	failureDetector := &FailureDetector{
		Cache:           Cache,
		RefreshRate:     refreshRate,
		CleanUpInterval: cleanUpInterval,
		ShutdownCh:      make(chan bool, 1),
	}

	flushingTicker := time.NewTicker(failureDetector.CleanUpInterval)
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
	return failureDetector
}

func (fd *FailureDetector) Store(data NetworkData) error {
	var (
		key  = storePrefix + string(data.IpAddress)
		ttls = []float64{}
	)
	result, err := fd.Cache.HMGet(key, "*").Result()

	if err != nil {
		return err
	}

	if result != nil {
		for i := 0; i < len(result); i++ {
			val, ok := result[i].(float64)
			if !ok {
				continue
			}
			ttls = append(ttls, val)
		}
	}

	if data.RoundRobinTrip == 0 {
		timeToForwardDestination := data.ForwardTrip.RecievedAt.Seconds() - data.ForwardTrip.SentAt.Seconds()
		timeToBackwardDestination := data.BackwardTrip.RecievedAt.Seconds() - data.BackwardTrip.SentAt.Seconds()
		data.RoundRobinTrip = time.Duration(timeToBackwardDestination + timeToForwardDestination)
	}
	ttls = append(ttls, data.RoundRobinTrip.Seconds())
	return fd.Cache.HSet(key, "*", ttls).Err()
}

func errorFunc(x, stdDev, variance float64) float64 {
	return math.Exp(-math.Pow((x-stdDev), 2.0) / (2 * variance))
}

func (fd *FailureDetector) Predict(addr string, windowSize int) (Prediction, error) {
	// - First, heartbeats arrive and their arrival times are stored in a sampling window.
	window, err := fd.sample(addr, windowSize)
	if err != nil {
		return Prediction{}, errors.New(ErrorHistoricalSampling)
	}

	if len(window) < 2 {
		return Prediction{}, errors.New(ErrorNotEnoughistoricalSampling)
	}
	// - Second,these past samples are used to determine the distributionof inter-arrival times.
	mean := utils.Sum(window) / float64(len(window))
	totalSquaredDeviation := 0.0

	utils.ForEach(window, func(v float64) {
		totalSquaredDeviation += math.Pow(v-mean, 2.0)
	})

	variance := totalSquaredDeviation / float64(len(window))
	stdDev := math.Sqrt(variance)
	tNow := time.Since(time.Time{}).Seconds()
	tLast := window[len(window)-2]

	// - Third, the distribution is in turn used to compute the current value of ϕ.
	probablityOfGettingHBLater := (1 / (stdDev * math.Sqrt(2*math.Pi))) * errorFunc(tNow-tLast, stdDev, variance)

	// suspicion level
	phil := -(math.Log10(probablityOfGettingHBLater))

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
	var (
		key  = storePrefix + addr
		ttls = []float64{}
	)
	result, err := fd.Cache.HMGet(key, "*").Result()
	if err != nil {
		return nil, err
	}

	if result != nil {
		for i := 0; i < len(result); i++ {
			val, ok := result[i].(float64)
			if !ok {
				continue
			}
			ttls = append(ttls, val)
		}
	}

	var sampleSpace []float64

	for i, j := windowSize-1, len(ttls)-1; windowSize > 0 && j > 0; {
		sampleSpace = append(sampleSpace, ttls[j])
		j -= 1
		i -= 1
	}

	return sampleSpace, nil
}
