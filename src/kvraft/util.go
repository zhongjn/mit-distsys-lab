package raftkv

import "log"

// Debug print?
const Debug = 0

// DPrintf prints debug information
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
