package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "fmt"
import "strconv"
import "sync"

type Task struct {
	Type string
	Args []string
}

type Master struct {
	// XXX Your definitions here.

	NReduce int
	Files   []string
}

var mapChan = make(chan Task, 10)
var wg sync.WaitGroup

// XXX Your code here -- RPC handlers for the worker to call.

func (m *Master) GetTask(args *TaskArgs, reply *TaskReply) error {
	fmt.Println("got task request")
	task := <-mapChan
	reply.TaskId = task.Args[0]
	reply.File = task.Args[1]
	reply.NReduce = m.NReduce
	return nil
}

func (m *Master) CompleteTask(args *TaskArgs, reply *TaskReply) error {
	fmt.Println("got task complete")
	fmt.Printf("complete args: %v\n", args.Files)
	wg.Done()
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
	fmt.Printf("listening on %v\n", sockname)
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	c := make(chan int)

	go func() {
		wg.Wait()
		close(c)
	}()

	select {
	case <-c:
		fmt.Println("finished")
		close(mapChan)
		ret = true
	case <-time.After(30 * time.Second):
		fmt.Println("timed out after 30 seconds")
		close(mapChan)
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
//
func MakeMaster(files []string, nReduce int) *Master {
	// XXX Your code here.

	// add Map tasks, 1 per file to the work queue
	go func() {
		fmt.Printf("map input: %v\n", files)
		for i := 0; i < len(files); i++ {
			task := Task{Type: "Map", Args: []string{strconv.Itoa(i), files[i]}}
			wg.Add(1)
			mapChan <- task
			fmt.Printf("queue map job: %v\n", files[i])
		}

		wg.Wait()

		fmt.Println("finished map phase")
		// FIXME dont exit after map is complete, wait on another group of reduce tasks

		// TODO collect map output and create reduce tasks with them
		// TODO send reduce jobs to channel
	}()

	m := Master{Files: files, NReduce: nReduce}

	// on worker get task, remove task from idle Map queue, and add it to in-progress worker queue
	// when worker completes, it sends back results
	// on worker complete, remove task from in-progress queue, and add it to the completed queue
	// sync on all the Map tasks finishing
	// add Reduce tasks, 1 per nBuckets to the idle Reduce queue
	// ...tbd

	m.server()
	return &m
}
