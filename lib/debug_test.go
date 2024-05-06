package lib

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

func TestDebug(t *testing.T) {
	buffer := bytes.NewBuffer([]byte{})

	rf := struct {
		me string
	}{me: "Tester12"}

	verbosity := getLogVerbosity()

	WDebug(buffer, dTimer, "S%d Leader, checking heartbeats", rf.me)

	logMsg := string(buffer.Bytes())

	if verbosity <= 0 {
		if !(len(logMsg) == 0) {
			t.Fatalf("expected an empty message as debug verbosity should be < 1")
		}
	} else {
		search := "checking heartbeats"
		if !strings.Contains(logMsg, search) {
			t.Fatalf("expected message to contain %v", search)
		}
		log.Println(logMsg)
	}
}
