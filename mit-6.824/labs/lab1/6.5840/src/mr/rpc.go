package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y         int
	Filenames []string
	NReduce   int
}

type MapTask struct {
	InputFile     string
	MapTaskNumber int
	ReduceCount   int
}

type ReduceTask struct {
	IntermediateFiles []string
	ReduceCount       int
}

type RequestTaskArgs struct {
	Pid int
}

type RequestTaskReply struct {
	MapTask    *MapTask
	ReduceTask *ReduceTask
	Done       bool
}

type UpdateMapTaskArgs struct {
	InputFile        string
	IntermediateFile []string
	Pid              int
}

type UpdateMapTaskReply struct {
}

type UpdateReduceTaskArgs struct {
	Pid          int
	ReduceNumber int
}

type UpdateReduceTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
