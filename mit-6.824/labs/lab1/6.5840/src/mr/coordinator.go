package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	TaskStatusUndefined TaskStatus = iota
	TaskStatusPending
	TaskStatusRunning
	TaskStatusCompleted
	TaskStatusFailed
	TaskStatusIdle
	TaskStatusBusy
)

type Status struct {
	StartTime int64
	Status    TaskStatus
}

// Arguments for updating the status of a map task.
type UpdateMapTaskStatusArgs struct {
	InputFile        string
	IntermediateFile string
	TaskID           int
}

// Arguments for updating the status of a reduce task.
type UpdateReduceTaskStatusArgs struct {
	TaskID int
	Status TaskStatus
}

// Reply for updating task status RPC calls.
type UpdateTaskStatusReply struct {
	// Add fields if needed
}

type Coordinator struct {
	// Your definitions here.
	filesLock         sync.Mutex
	workerStatus      map[int]TaskStatus
	files             []string
	intermediateFiles map[int][]string
	nReduce           int
	mapTaskStatus     map[string]Status
	mapTaskNumber     int
	reduceTaskStatus  map[int]Status
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.filesLock.Lock()

	if c.workerStatus[args.Pid] == TaskStatusUndefined {
		c.workerStatus[args.Pid] = TaskStatusIdle
	}

	var mapTask *MapTask = nil
	for k, v := range c.mapTaskStatus {
		if v.Status == TaskStatusPending {
			mapTask = &MapTask{InputFile: k, MapTaskNumber: c.mapTaskNumber, ReduceCount: c.nReduce}
			c.mapTaskStatus[k] = Status{Status: TaskStatusRunning, StartTime: time.Now().Unix()}
			c.mapTaskNumber++
			break
		}
	}

	if mapTask != nil {
		reply.MapTask = mapTask
		reply.Done = false
		c.workerStatus[args.Pid] = TaskStatusBusy
		c.filesLock.Unlock()
		return nil
	}

	if !c.MapDone() {
		reply.MapTask = mapTask
		reply.Done = false
		c.filesLock.Unlock()
		return nil
	}

	var reduceTask *ReduceTask = nil
	var reduceIndex = -1

	for k, v := range c.reduceTaskStatus {
		if v.Status == TaskStatusPending {
			reduceIndex = k
			break
		}
	}

	if reduceIndex >= 0 {
		reduceTask = &ReduceTask{ReduceCount: reduceIndex, IntermediateFiles: c.intermediateFiles[reduceIndex]}
		c.reduceTaskStatus[reduceIndex] = Status{Status: TaskStatusRunning, StartTime: time.Now().Unix()}
	}

	if reduceTask != nil {
		reply.ReduceTask = reduceTask
		reply.Done = false
		c.workerStatus[args.Pid] = TaskStatusBusy
		c.filesLock.Unlock()
		return nil
	}

	c.filesLock.Unlock()
	reply.Done = c.Done()
	return nil
}

func (c *Coordinator) UpdateMapTask(args UpdateMapTaskArgs, reply *UpdateMapTaskReply) error {
	c.filesLock.Lock()
	defer c.filesLock.Unlock()

	c.workerStatus[args.Pid] = TaskStatusIdle
	c.mapTaskStatus[args.InputFile] = Status{Status: TaskStatusCompleted, StartTime: -1}
	for r := 0; r < c.nReduce; r++ {
		c.intermediateFiles[r] = append(c.intermediateFiles[r], args.IntermediateFile[r])
	}
	return nil
}

func (c *Coordinator) UpdateReduceTask(args UpdateReduceTaskArgs, reply *UpdateReduceTaskReply) error {
	c.filesLock.Lock()
	defer c.filesLock.Unlock()

	c.workerStatus[args.Pid] = TaskStatusIdle
	c.reduceTaskStatus[args.ReduceNumber] = Status{Status: TaskStatusCompleted, StartTime: -1}

	return nil
}

func (c *Coordinator) RunWorkerTimeout() {
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				if c.Done() {
					return
				}
				c.CheckWorkerTimeout()
			}
		}
	}()
}

func (c *Coordinator) CheckWorkerTimeout() {
	c.filesLock.Lock()
	defer c.filesLock.Unlock()
	for k, v := range c.mapTaskStatus {
		now := time.Now().Unix()
		if v.Status == TaskStatusRunning {
			if v.StartTime > 0 && now > (v.StartTime+10) {
				c.mapTaskStatus[k] = Status{Status: TaskStatusPending, StartTime: -1}
			}
		}
	}

	for k, v := range c.reduceTaskStatus {
		now := time.Now().Unix()
		if v.Status == TaskStatusRunning {
			if v.StartTime > 0 && now > (v.StartTime+10) {
				c.reduceTaskStatus[k] = Status{Status: TaskStatusPending, StartTime: -1}
			}
		}
	}

}

// start a thread that listens for RPCs from worker.go
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
	go func() {
		if err := http.Serve(l, nil); err != nil {
			log.Fatal("serve error:", err)
		}
	}()
}

func (c *Coordinator) MapDone() bool {
	// Your code here.
	ret := true

	for _, v := range c.mapTaskStatus {
		if v.Status != TaskStatusCompleted {
			ret = false
		}
	}
	return ret
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.filesLock.Lock()
	defer c.filesLock.Unlock()
	ret := true

	// // Check if all map tasks have completed
	// for _, task := range c.mapTaskStatus {
	// 	if task.Status != TaskStatusCompleted {
	// 		return false
	// 	}
	// }

	// Check if all reduce tasks have completed
	for _, task := range c.reduceTaskStatus {
		if task.Status != TaskStatusCompleted {
			return false
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		files:            files,
		nReduce:          nReduce,
		workerStatus:     make(map[int]TaskStatus),
		mapTaskNumber:    0,
		mapTaskStatus:    make(map[string]Status),
		reduceTaskStatus: make(map[int]Status),
	}

	for _, v := range files {
		c.mapTaskStatus[v] = Status{Status: TaskStatusPending, StartTime: -1}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTaskStatus[i] = Status{Status: TaskStatusPending, StartTime: -1}
	}

	c.RunWorkerTimeout()
	c.server()
	return &c
}
