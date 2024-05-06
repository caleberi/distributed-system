package lib

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

func getLogVerbosity() int {
	verbosity := os.Getenv("VERBOSE")
	if verbosity == "" {
		return 0
	}
	level, err := strconv.Atoi(verbosity)
	if err != nil {
		log.Fatalf("Invalid verbosity %v", verbosity)
	}
	return level
}

type debugTopic string

var debugStartTime time.Time
var debugVerbosity int

const (
	dClient  debugTopic = "CLNT"
	dCommit  debugTopic = "CMIT"
	dDrop    debugTopic = "DROP"
	dError   debugTopic = "ERROR"
	dInfo    debugTopic = "INFO"
	dLeader  debugTopic = "LEAD"
	dLog     debugTopic = "LOG-1"
	dLog2    debugTopic = "LOG_2"
	dPersist debugTopic = "PERS"
	dSnap    debugTopic = "SNAP"
	dTerm    debugTopic = "TERM"
	dTest    debugTopic = "TEST"
	dTimer   debugTopic = "TIMER"
	dTrace   debugTopic = "TRACE"
	dVote    debugTopic = "VOTE"
	dWarn    debugTopic = "WARN"
)

func init() {
	debugVerbosity = getLogVerbosity()
	debugStartTime = time.Now()

	// turn off log.date & log time
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic debugTopic, format string, args ...interface{}) {
	WDebug(os.Stdout, topic, format, args)
}

func WDebug(w io.Writer, topic debugTopic, format string, args ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStartTime).Milliseconds()
		time /= 1000
		prefix := fmt.Sprintf("%06d %v", time, string(topic))
		format = prefix + format
		fmt.Fprintf(w, format, args...)
	}
}
