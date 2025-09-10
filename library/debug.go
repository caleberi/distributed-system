package library

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
	DClient  debugTopic = "CLNT"
	DCommit  debugTopic = "CMIT"
	DDrop    debugTopic = "DROP"
	DError   debugTopic = "ERROR"
	DInfo    debugTopic = "INFO"
	DLeader  debugTopic = "LEAD"
	DLog     debugTopic = "LOG-1"
	DLog2    debugTopic = "LOG_2"
	DPersist debugTopic = "PERS"
	DSnap    debugTopic = "SNAP"
	DTerm    debugTopic = "TERM"
	DTest    debugTopic = "TEST"
	DTimer   debugTopic = "TIMER"
	DTrace   debugTopic = "TRACE"
	DVote    debugTopic = "VOTE"
	DWarn    debugTopic = "WARN"
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
