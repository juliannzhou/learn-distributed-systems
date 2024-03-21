package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	filenames, nReduce, err := CallExample()
	if err != nil {
		log.Fatalf("failed to get filenames and NReduce: %v", err)
	}

	intermediate := make([][]KeyValue, nReduce)
	for i, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))

		args := UpdateMapTaskStatusArgs{
			TaskID: i,
			Status: TaskStatusCompleted,
		}
		reply := UpdateTaskStatusReply{}

		if !call("Coordinator.UpdateMapTaskStatus", &args, &reply) {
			log.Fatalf("failed to update map task status")
		}

		for _, kv := range kva {
			reduceTask := ihash(kv.Key) % nReduce
			intermediate[reduceTask] = append(intermediate[reduceTask], kv)
		}
	}

	for reduceTask, kvs := range intermediate {
		values := make([]string, len(kvs))
		for i, kv := range kvs {
			values[i] = kv.Value
		}

		output := reducef(kvs[0].Key, values)

		reduceFileName := fmt.Sprintf("mr-out%d", reduceTask)
		reduceFile, err := os.Create(reduceFileName)

		args := UpdateReduceTaskStatusArgs{
			TaskID: reduceTask,
			Status: TaskStatusCompleted, // Or TaskStatusFailed if an error occurred
		}
		reply := UpdateTaskStatusReply{}
		if !call("Coordinator.UpdateReduceTaskStatus", &args, &reply) {
			log.Fatalf("failed to update map task status")
		}

		if err != nil {
			log.Fatalf("cannot create reduce output file %s: %v", reduceFileName, err)
		}
		defer reduceFile.Close()

		fmt.Fprintf(reduceFile, "%v\n", output)
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() ([]string, int, error) {

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
		return reply.Filenames, reply.NReduce, nil
	} else {
		fmt.Printf("call failed!\n")
		return nil, -1, nil
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
