package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskRequest struct {
	WorkerId int // 可以用来标识不同的 Worker
}

type TaskResponse struct {
	TaskType  int // 0 for Map, 1 for Reduce, 2 for Exit
	TaskId    int
	InputFile string   // For Map tasks
	ReduceFiles []string // For Reduce tasks
	NReduce   int    // Total number of reduce tasks
}

type TaskCompleteRequest struct {
	TaskType int
	TaskId   int
	Success  bool
}

type TaskCompleteResponse struct {
	// Nothing needed for now
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
