package library

import (
	"bytes"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDebug(t *testing.T) {
	buffer := bytes.NewBuffer([]byte{})

	rf := struct {
		me string
	}{me: "Tester12"}

	verbosity := getLogVerbosity()

	WDebug(buffer, DTimer, "S%d Leader, checking heartbeats", rf.me)

	logMsg := buffer.String()

	if verbosity <= 0 {
		assert.Equal(t, 0, len(logMsg), "expected an empty message as debug verbosity should be < 1")
	} else {
		search := "checking heartbeats"
		assert.NotContainsf(t, logMsg, search, "expected message to contain %v", search)
		log.Println(logMsg)
	}
}
