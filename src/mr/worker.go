package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"time"
	"os"
	"io/ioutil"
	"encoding/json"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// Generate a unique worker ID (e.g., using PID)
	workerId := os.Getpid()

	for {
		// Request a task from the coordinator
		args := TaskRequest{WorkerId: workerId}
		reply := TaskResponse{}
		ok := call("Coordinator.RequestTask", &args, &reply)

		if !ok {
			// Coordinator is likely done or crashed, exit
			log.Printf("Worker %d: Coordinator is not available, exiting.\n", workerId)
			break
		}

		switch reply.TaskType {
		case MapTask:
			log.Printf("Worker %d: Received Map task %d on file %s\n", workerId, reply.TaskId, reply.InputFile)
			// Execute Map task
			intermediate := []KeyValue{}

			// Read input file (borrowed from mrsequential.go)
			file, err := os.Open(reply.InputFile)
			if err != nil {
				log.Fatalf("cannot open %v", reply.InputFile)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.InputFile)
			}
			file.Close()

			// Call the application Map function
			kva := mapf(reply.InputFile, string(content))
			intermediate = append(intermediate, kva...)

			// Create intermediate files for each reduce task
			// Group key/value pairs by reduce task number
			reduceBuckets := make([][]KeyValue, reply.NReduce)
			for _, kv := range intermediate {
				reduceId := ihash(kv.Key) % reply.NReduce
				reduceBuckets[reduceId] = append(reduceBuckets[reduceId], kv)
			}

			// Write intermediate files
			for i := 0; i < reply.NReduce; i++ {
				// Use temporary file and rename for atomicity
				oname := fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
				tmpFile, err := ioutil.TempFile(".", "mr-tmp-")
				if err != nil {
					log.Fatalf("cannot create temporary file: %v", err)
				}

				enc := json.NewEncoder(tmpFile)
				for _, kv := range reduceBuckets[i] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encode intermediate key/value: %v", err)
					}
				}
				tmpFile.Close()

				os.Rename(tmpFile.Name(), oname)
			}

			// Report task completion
			completeArgs := TaskCompleteRequest{TaskType: MapTask, TaskId: reply.TaskId, Success: true}
			completeReply := TaskCompleteResponse{}
			call("Coordinator.TaskComplete", &completeArgs, &completeReply)

		case ReduceTask:
			log.Printf("Worker %d: Received Reduce task %d\n", workerId, reply.TaskId)
			// Execute Reduce task

			// Read intermediate files for this reduce task
			intermediate := []KeyValue{}
			for _, filename := range reply.ReduceFiles {
				file, err := os.Open(filename)
				if err != nil {
					log.Printf("Warning: Cannot open intermediate file %v: %v\n", filename, err)
					continue // Skip missing files, coordinator will reassign if necessary
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}

			// Sort intermediate key/value pairs by key
			sort.Sort(ByKey(intermediate))

			// Call the application Reduce function
			oname := fmt.Sprintf("mr-out-%d", reply.TaskId)
			// Use temporary file and rename for atomicity
			tmpFile, err := ioutil.TempFile(".", "mr-tmp-")
			if err != nil {
				log.Fatalf("cannot create temporary file: %v", err)
			}

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format (borrowed from mrsequential.go)
				fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			tmpFile.Close()

			os.Rename(tmpFile.Name(), oname)

			// Report task completion
			completeArgs := TaskCompleteRequest{TaskType: ReduceTask, TaskId: reply.TaskId, Success: true}
			completeReply := TaskCompleteResponse{}
			call("Coordinator.TaskComplete", &completeArgs, &completeReply)

		case ExitTask:
			log.Printf("Worker %d: Received Exit task, exiting.\n", workerId)
			return // Exit the worker loop

		case WaitTask:
			// No tasks available, wait and retry
			log.Printf("Worker %d: No tasks available, waiting.\n", workerId)
			time.Sleep(time.Second)

		default:
			log.Printf("Worker %d: Received unknown task type %d\n", workerId, reply.TaskType)
			// Should not happen, wait and retry
			time.Sleep(time.Second)
		}

	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		//log.Fatal("dialing:", err)
		return false // Coordinator is likely down
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}
