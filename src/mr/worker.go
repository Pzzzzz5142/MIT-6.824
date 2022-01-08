package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	args := AssignJobArgs{}
	reply := AssignJobReply{}

	for call("Coordinator.AssignJob", &args, &reply) {
		mapJobs := reply.MapJobs
		reduceJobs := reply.ReduceJobs
		fileName := ""
		switch reply.JobType {
		case "mapping":
			fileName = fmt.Sprintf("mr-%v", reply.JobId)
			file, err := os.CreateTemp(".", "*-"+fileName)
			if err != nil {
				log.Fatalf("cannot open %v", fileName)
			}
			enc := json.NewEncoder(file)
			for _, filename := range mapJobs {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				output := mapf(filename, string(content))
				for _, v := range output {
					err := enc.Encode(&v)
					if err != nil {
						log.Fatalln("cannot encode", v)
					}
				}
			}
			file.Close()
			os.Rename("./"+file.Name(), "./"+fileName)
		case "reducing":
			fileName = fmt.Sprintf("mr-out%v", reply.JobId)
			ofile, _ := os.CreateTemp(".", "*-"+fileName)
			for _, job := range reduceJobs {
				key := job[0]
				values := job[1:]
				output := reducef(key, values)
				fmt.Fprintf(ofile, "%v %v\n", key, output)
			}
			ofile.Close()
			os.Rename("./"+ofile.Name(), "./"+fileName)
		case "quit":
			os.Exit(0)
		}

		finishedArgs := FinishJobArgs{mapJobs, reduceJobs, reply.JobType, fileName}
		finishedReply := FinishJobReply{}
		call("Coordinator.FinishJob", &finishedArgs, &finishedReply)
		if finishedReply.Abandoned {
			os.Remove(fileName)
		}
		time.Sleep(time.Millisecond)
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
