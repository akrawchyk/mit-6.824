package mr

import "io/ioutil"
import "os"
import "fmt"
import "log"
import "sort"
import "net/rpc"
import "hash/fnv"

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

	// TODO
	// One way to get started is to modify mr/worker.go's Worker()
	// to send an RPC to the master asking for a task. Then modify
	// the master to respond with the file name of an
	// as-yet-unstarted map task. Then modify the worker to read
	// that file and call the application Map function, as in
	// mrsequential.go.
	intermediate := []KeyValue{}
	reply := CallGetTask()
	fmt.Printf("CallGetTask: %v", reply)
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

	sort.Sort(ByKey(intermediate))

	// FIXME write to partitions, not chunks over the bucket size
	//       this way, each reduce worker will collect all of the same
	//       keys in the files it's assigned
	// chunk intermediate into buckets
	chunkSize := (len(intermediate) + nReduce - 1) / nReduce
	buckets := [][]KeyValue{}
	for i := 0; i < len(intermediate); i += chunkSize {
		end := i + chunkSize

		if end > len(intermediate) {
			end = len(intermediate)
		}

		buckets = append(buckets, intermediate[i:end])
	}

	// write to intermediate files
	for i := 0; i < len(buckets); i++ {
		// loop over chunk to write a file
		chunk := buckets[i]

		// TODO get worker number from master
		oname := fmt.Sprintf("mr-intermediate-%d-%d", 0, i)
		ofile, _ := os.Create(oname)

		for j := 0; j < len(chunk); j++ {
			fmt.Fprintf(ofile, "{\"%v\":\"%v\"}\n", chunk[j].Key, chunk[j].Value)
		}

		ofile.Close()
	}

}

func CallGetTask() TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}
	call("Master.GetTask", &args, &reply)
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
