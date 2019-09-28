package raft

import "log"

// Debugging
const Debug = 0
const InfoPrint = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func InfoPrintf(format string, a ...interface{}) (n int, err error) {
	if InfoPrint > 0 {
		log.Printf(format, a...)
	}
	return
}
