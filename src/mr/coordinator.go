package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

const (
	MapTask = 0
	ReduceTask = 1
	ExitTask = 2
	WaitTask = 3 // Indicate worker to wait and retry
)

type Task struct {
	Id          int
	Type        int // 0 for Map, 1 for Reduce
	InputFile   string // For Map tasks
	ReduceFiles []string // For Reduce tasks
	State       TaskState
	Start_Time  time.Time // 使用时间戳记录任务开始时间，用于故障检测
}

type Coordinator struct {
	// Your definitions here.
	Files []string
	NReduce int
	MapTasks []Task
	ReduceTasks []Task
	Mutex sync.Mutex
	MapTaskCount int // 用于生成 Map 任务 ID
	ReduceTaskCount int // 用于生成 Reduce 任务 ID
	MapCompletedCount int
	ReduceCompletedCount int

	IntermediateFiles [][]string // IntermediateFiles[reduceTaskId] = list of files from map tasks
}

// Your code here -- RPC handlers for the worker to call.

// RequestTask RPC handler
func (c *Coordinator) RequestTask(args *TaskRequest, reply *TaskResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// Check for Map tasks
	for i := 0; i < len(c.MapTasks); i++ {
		if c.MapTasks[i].State == Idle || (c.MapTasks[i].State == InProgress && time.Since(c.MapTasks[i].Start_Time) > 10*time.Second) {
			reply.TaskType = MapTask
			reply.TaskId = c.MapTasks[i].Id
			reply.InputFile = c.MapTasks[i].InputFile
			reply.NReduce = c.NReduce
			c.MapTasks[i].State = InProgress
			c.MapTasks[i].Start_Time = time.Now()
			log.Printf("Assigned Map task %d to worker %d\n", reply.TaskId, args.WorkerId)
			return nil
		}
	}

	// Check if all Map tasks are completed
	if c.MapCompletedCount < len(c.Files) {
		// Map tasks are still in progress, tell worker to wait
		reply.TaskType = WaitTask
		log.Printf("No Map tasks available, telling worker %d to wait\n", args.WorkerId)
		return nil
	}

	// Check for Reduce tasks
	for i := 0; i < len(c.ReduceTasks); i++ {
		if c.ReduceTasks[i].State == Idle || (c.ReduceTasks[i].State == InProgress && time.Since(c.ReduceTasks[i].Start_Time) > 10*time.Second) {
			reply.TaskType = ReduceTask
			reply.TaskId = c.ReduceTasks[i].Id
			reply.ReduceFiles = c.IntermediateFiles[i] // Assign intermediate files for this reduce task
			c.ReduceTasks[i].State = InProgress
			c.ReduceTasks[i].Start_Time = time.Now()
			log.Printf("Assigned Reduce task %d to worker %d\n", reply.TaskId, args.WorkerId)
			return nil
		}
	}

	// Check if all tasks are completed
	if c.ReduceCompletedCount == c.NReduce {
		// All tasks done, tell worker to exit
		reply.TaskType = ExitTask
		log.Printf("All tasks completed, telling worker %d to exit\n", args.WorkerId)
		return nil
	}

	// If no tasks are available and not all tasks are done, tell worker to wait
	reply.TaskType = WaitTask
	log.Printf("No tasks available, telling worker %d to wait\n", args.WorkerId)
	return nil
}

// TaskComplete RPC handler
func (c *Coordinator) TaskComplete(args *TaskCompleteRequest, reply *TaskCompleteResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if args.TaskType == MapTask {
		if c.MapTasks[args.TaskId].State == InProgress {
			c.MapTasks[args.TaskId].State = Completed
			c.MapCompletedCount++
			log.Printf("Map task %d completed\n", args.TaskId)

			// Collect intermediate file info (assuming worker names files as mr-mapId-reduceId)
			for i := 0; i < c.NReduce; i++ {
				c.IntermediateFiles[i] = append(c.IntermediateFiles[i], fmt.Sprintf("mr-%d-%d", args.TaskId, i))
			}
		}
	} else if args.TaskType == ReduceTask {
		if c.ReduceTasks[args.TaskId].State == InProgress {
			c.ReduceTasks[args.TaskId].State = Completed
			c.ReduceCompletedCount++
			log.Printf("Reduce task %d completed\n", args.TaskId)
		}
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	ret := false
	// Your code here.
	if c.MapCompletedCount == len(c.Files) && c.ReduceCompletedCount == c.NReduce {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Files = files
	c.NReduce = nReduce
	c.MapTasks = make([]Task, len(files))
	c.MapTaskCount = len(files)
	c.ReduceTasks = make([]Task, nReduce)
	c.ReduceTaskCount = nReduce
	c.MapCompletedCount = 0
	c.ReduceCompletedCount = 0

	c.IntermediateFiles = make([][]string, nReduce)
	for i := 0; i < c.NReduce; i++ {
		c.IntermediateFiles[i] = []string{}
	}

	// Initialize Map tasks
	for i, file := range files {
		c.MapTasks[i] = Task{
			Id:        i,
			Type:      MapTask, // Map task
			InputFile: file,
			State:     Idle,
			Start_Time: time.Time{}, // Initialize with zero time
		}
	}

	// Initialize Reduce tasks (initially Idle)
	for i := 0; i < c.NReduce; i++ {
		c.ReduceTasks[i] = Task{
			Id:    i,
			Type:  ReduceTask, // Reduce task
			State: Idle,
			Start_Time: time.Time{}, // Initialize with zero time
		}
	}


	c.server()
	return &c
}
