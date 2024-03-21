package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type TaskStatus int

const (
	TaskStatusPending TaskStatus = iota
	TaskStatusRunning
	TaskStatusCompleted
	TaskStatusFailed
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
)

type Task struct {
	ID     int
	Type   TaskType
	Status TaskStatus
}

// Arguments for updating the status of a map task.
type UpdateMapTaskStatusArgs struct {
	TaskID int
	Status TaskStatus
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
	filesLock   sync.Mutex
	files       []string
	nReduce     int
	mapTasks    []*Task
	reduceTasks []*Task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	c.filesLock.Lock()
	defer c.filesLock.Unlock()

	reply.Y = args.X + 1
	reply.Filenames = c.files
	reply.NReduce = c.nReduce
	return nil
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

func (c *Coordinator) UpdateMapTaskStatus(args *UpdateMapTaskStatusArgs, reply *UpdateTaskStatusReply) error {
	if args.TaskID < 0 || args.TaskID >= len(c.mapTasks) {
		return fmt.Errorf("invalid map task ID")
	}
	c.mapTasks[args.TaskID].Status = args.Status
	return nil
}

func (c *Coordinator) UpdateReduceTaskStatus(args *UpdateReduceTaskStatusArgs, reply *UpdateTaskStatusReply) error {
	if args.TaskID < 0 || args.TaskID >= len(c.reduceTasks) {
		return fmt.Errorf("invalid reduce task ID")
	}
	c.reduceTasks[args.TaskID].Status = args.Status
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.filesLock.Lock()
	defer c.filesLock.Unlock()

	// Check if all map tasks have completed
	for _, task := range c.mapTasks {
		if task.Status != TaskStatusCompleted {
			return false
		}
	}

	// Check if all reduce tasks have completed
	for _, task := range c.reduceTasks {
		if task.Status != TaskStatusCompleted {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		mapTasks:    make([]*Task, len(files)),
		reduceTasks: make([]*Task, nReduce),
	}

	for i := range c.mapTasks {
		c.mapTasks[i] = &Task{ID: i, Type: MapTask, Status: TaskStatusPending}
	}

	for i := range c.reduceTasks {
		c.reduceTasks[i] = &Task{ID: i, Type: ReduceTask, Status: TaskStatusPending}
	}

	c.server()
	return &c
}
