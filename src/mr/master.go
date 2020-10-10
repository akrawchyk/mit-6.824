package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "fmt"

type Master struct {
	// XXX Your definitions here.

	NReduce int
	Files   []string
	// storage for each map and reduce task
	//Tasks map[int]struct{ task, status string }
	// storage for locations and sizes of intermediate output
}

// XXX Your code here -- RPC handlers for the worker to call.

func (m *Master) GetTask(args *TaskArgs, reply *TaskReply) error {
	fmt.Println("master: got task request")
	reply.File = m.Files[0]
	reply.NReduce = 10
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	var c chan int

	// XXX Your code here.

	// TODO check if all tasks in storage are in `completed` state

	// added a global timeout
	select {
	case m := <-c:
		fmt.Println("waiting...", m)
	case <-time.After(30 * time.Second):
		fmt.Println("timed out after 30 seconds")
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{Files: files, NReduce: nReduce}

	fmt.Printf("Using %d workers\n", m.NReduce)

	// XXX Your code here.

	// listen for a worker to ask for a task
	// wait for the worker to finish the task
	//   or timeout after 10 seconds,
	//   and we retry task with a different worker

	m.server()
	return &m
}
