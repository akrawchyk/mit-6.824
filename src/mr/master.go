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
import "strings"

type Task struct {
	Id string
	Type string
	Args []string
}

type Master struct {
	// XXX Your definitions here.

	NReduce           int
	Files             []string
	IntermediateFiles []string
}

var taskChan = make(chan Task, 10)
var wg sync.WaitGroup
var tg sync.WaitGroup

// XXX Your code here -- RPC handlers for the worker to call.

func (m *Master) GetTask(args *TaskArgs, reply *TaskReply) error {
	task := <-taskChan
	// TODO put task on in-progress list and re-add it to the taskChan if it isn't removed in x time
	reply.TaskType = task.Type
	reply.TaskId = task.Id

	if task.Type == "Map" {
		reply.Args = []string{strconv.Itoa(m.NReduce), task.Args[0]}
	} else if task.Type == "Reduce" {
		reply.Args = task.Args
	} else {
		fmt.Printf("unexpected task type %v\n", task.Type)
	}
	fmt.Println("got task request")
	return nil
}

func (m *Master) CompleteTask(args *TaskArgs, reply *TaskReply) error {
	fmt.Println("got task complete")
	fmt.Printf("complete args: %v\n", args.Files)
	m.IntermediateFiles = append(m.IntermediateFiles, args.Files...)
	tg.Done()
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
		close(taskChan)
		ret = true
	case <-time.After(30 * time.Second):
		fmt.Println("timed out after 30 seconds")
		close(taskChan)
		ret = true
	}

	return ret
}

// Filter returns a new slice holding only
// the elements of s that satisfy fn()
// see https://blog.golang.org/slices-intro
func Filter(s []string, fn func(string) bool) []string {
	var p []string // == nil
	for _, v := range s {
		if fn(v) {
			p = append(p, v)
		}
	}
	return p
}

//
// create a Master.
// main/mrmaster.go calls this function.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{Files: files, NReduce: nReduce}

	// XXX Your code here.
	wg.Add(2) // two phases: map and reduce

	go func() {
		// add Map tasks, 1 per file to the work queue
		fmt.Printf("map input: %v\n", files)
		for i := 0; i < len(files); i++ {
			mTask := Task{Type: "Map", Id: strconv.Itoa(i), Args: []string{files[i]}}
			tg.Add(1)
			taskChan <- mTask
			fmt.Printf("queue map job: %v\n", mTask)
		}

		tg.Wait()
		fmt.Println("finished map phase")
		wg.Done() // map phase done

		// FIXME dont exit after map is complete, wait on another group of reduce tasks

		// collect map output and create reduce tasks with them
		for i := 0; i < nReduce; i++ {
			reduceFiles := Filter(m.IntermediateFiles, func(str string) bool {
				return strings.HasSuffix(str, strconv.Itoa(i))
			})
			rTask := Task{Type: "Reduce", Id: strconv.Itoa(i), Args: reduceFiles}
			tg.Add(1)
			taskChan <- rTask
			fmt.Printf("queue reduce job: %v\n", rTask)
		}

		tg.Wait()
		fmt.Println("finished reduce phase")
		wg.Done() // reduce phase done
	}()
	// on worker get task, remove task from idle Map queue, and add it to in-progress worker queue
	// when worker completes, it sends back results
	// on worker complete, remove task from in-progress queue, and add it to the completed queue
	// sync on all the Map tasks finishing
	// add Reduce tasks, 1 per nBuckets to the idle Reduce queue
	// ...tbd

	m.server()
	return &m
}
