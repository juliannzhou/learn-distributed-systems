package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	stop := false
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for !stop {
		reply := RequestTask()

		if reply.Done {
			stop = true
			fmt.Println("all tasks done, exit...")
			continue
		}

		if reply.MapTask != nil {
			MapTaskHandler(reply.MapTask, mapf)
		}

		if reply.ReduceTask != nil {
			ReduceTaskHandler(reply.ReduceTask, reducef)
		}
	}

	// for reduceTask, kvs := range intermediate {
	// 	values := make([]string, len(kvs))
	// 	for i, kv := range kvs {
	// 		values[i] = kv.Value
	// 	}

	// 	output := reducef(kvs[0].Key, values)

	// 	reduceFileName := fmt.Sprintf("mr-out%d", reduceTask)
	// 	reduceFile, err := os.Create(reduceFileName)

	// 	args := UpdateReduceTaskStatusArgs{
	// 		TaskID: reduceTask,
	// 		Status: TaskStatusCompleted, // Or TaskStatusFailed if an error occurred
	// 	}
	// 	reply := UpdateTaskStatusReply{}
	// 	if !call("Coordinator.UpdateReduceTaskStatus", &args, &reply) {
	// 		log.Fatalf("failed to update map task status")
	// 	}

	// 	if err != nil {
	// 		log.Fatalf("cannot create reduce output file %s: %v", reduceFileName, err)
	// 	}
	// 	defer reduceFile.Close()

	// 	fmt.Fprintf(reduceFile, "%v\n", output)
	// }

}

func MapTaskHandler(task *MapTask, mapf func(string, string) []KeyValue) {
	filename := task.InputFile
	reduceCount := task.ReduceCount

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

	sort.Sort(ByKey(kva))
	partitionedKva := make([][]KeyValue, reduceCount)

	for _, v := range kva {
		partitionKey := ihash(v.Key) % reduceCount
		partitionedKva[partitionKey] = append(partitionedKva[partitionKey], v)
	}

	intermediateFiles := make([]string, reduceCount)

	for i := 0; i < reduceCount; i++ {
		intermediateFile := fmt.Sprintf("mr-%v-%v", task.MapTaskNumber, i)
		intermediateFiles[i] = intermediateFile
		ofile, _ := os.Create(intermediateFile)

		b, err := json.Marshal(partitionedKva[i])
		if err != nil {
			fmt.Println("Marshall error: ", err)
		}
		ofile.Write(b)

		ofile.Close()
	}

}

func ReduceTaskHandler(task *ReduceTask, reducef func(string, []string) string) {
	files := task.IntermediateFiles
	intermediate := []KeyValue{}

	for _, f := range files {
		dat, err := os.ReadFile(f)
		if err != nil {
			fmt.Println("Read error: ")
		}
		var input []KeyValue
		err = json.Unmarshal(dat, &input)
		if err != nil {
			fmt.Println("Unmarshal error: ", err.Error())
		}

		intermediate = append(intermediate, input...)
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", task.ReduceCount)
	temp, err := os.CreateTemp(".", oname)
	if err != nil {
		fmt.Println("Error creating temp file")
	}

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(temp, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	os.Rename(temp.Name(), oname)
}

func UpdateMapTask(args UpdateMapTaskArgs) UpdateMapTaskReply {
	reply := UpdateMapTaskReply{}
	call("Coordinator.UpdateMapTask", &args, &reply)
	return reply
}

func UpdateReduceTask(args UpdateReduceTaskArgs) UpdateReduceTaskReply {
	reply := UpdateReduceTaskReply{}
	call("Coordinator.UpdateReduceTask", &args, &reply)
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func RequestTask() RequestTaskReply {

	// declare an argument structure.
	args := RequestTaskArgs{}

	// fill in the argument(s).
	args.Pid = os.Getpid()

	// declare a reply structure.
	reply := RequestTaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	call("Coordinator.RequestTask", &args, &reply)
	return reply
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
