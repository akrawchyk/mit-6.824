package mr

import "io/ioutil"
import "os"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// XXX Your worker implementation here.
	intermediate := []KeyValue{}
	reply := CallGetTask()
	fmt.Printf("CallGetTask: %v", reply)
	taskId := reply.TaskId
	filename := reply.File
	nReduce := reply.NReduce
	fmt.Printf("%d\n", nReduce)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("worker: cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("worker: cannot read %v", filename)
	}

	file.Close()

	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// partition intermediate into buckets
	// this way, each reduce worker will collect all of the same
	// keys in the files it's assigned
	buckets := make([][]KeyValue, nReduce)

	for i := 0; i < len(intermediate); i++ {
		bucket := ihash(intermediate[i].Key) % nReduce
		buckets[bucket] = append(buckets[bucket], intermediate[i])
	}

	out := make([]string, nReduce)
	// write to intermediate files
	for i := 0; i < len(buckets); i++ {
		// loop over chunk to write a file
		chunk := buckets[i]

		oname := fmt.Sprintf("mr-intermediate-%v-%v", taskId, i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)

		for _, kv := range chunk {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("worker: cannot write intermediate output to %v", oname)
			}
		}

		out = append(out, oname)
		ofile.Close()
	}

	CallCompleteTask(out)
	fmt.Printf("CallCompleteTask: %v\n", reply)
}

func CallGetTask() TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}
	call("Master.GetTask", &args, &reply)
	return reply
}

func CallCompleteTask(out []string) TaskReply {
	args := TaskArgs{Files: out}
	reply := TaskReply{}
	call("Master.CompleteTask", &args, &reply)
	return reply
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
