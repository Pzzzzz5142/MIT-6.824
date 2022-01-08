package mr

import (
	"encoding/json"
	"errors"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type unfinishedJobs struct {
	MapJobs    []string
	ReduceJobs [][]string
	AssignTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	needMap          []string
	unfinishedMap    []unfinishedJobs
	needReduce       [][]string
	upperbound       int
	unfinishedReduce []unfinishedJobs
	nReduce          int
	mux              sync.Mutex
	jobInd           int
	state            string
	mapFiles         []string
}

// Your code here -- RPC handlers for the worker to call.

func (x *Coordinator) checkUnfinished() {
	j := 0
	for _, v := range x.unfinishedMap {
		if time.Since(v.AssignTime) > time.Second*10 {
			x.needMap = append(x.needMap, v.MapJobs...)
		} else {
			x.unfinishedMap[j] = v
			j += 1
		}
	}
	x.unfinishedMap = x.unfinishedMap[:j]
	j = 0
	for _, v := range x.unfinishedReduce {
		if time.Since(v.AssignTime) > time.Second*10 {
			x.needReduce = append(x.needReduce, v.ReduceJobs...)
		} else {
			x.unfinishedReduce[j] = v
			j += 1
		}
	}
	x.unfinishedReduce = x.unfinishedReduce[:j]
}

func (x *Coordinator) AssignJob(args *AssignJobArgs, reply *AssignJobReply) error {
	x.mux.Lock()
	defer x.mux.Unlock()
	switch x.state {
	case "map":
		if len(x.needMap) == 0 {
			reply.JobType = "idle"
			x.checkUnfinished()
		} else {
			reply.JobType = "mapping"
			upper_bound := int(math.Min(float64(len(x.needMap)), float64(x.upperbound)))
			reply.MapJobs = x.needMap[:upper_bound]
			reply.JobId = x.jobInd
			x.needMap = x.needMap[upper_bound:]
			x.unfinishedMap = append(x.unfinishedMap, unfinishedJobs{reply.MapJobs, nil, time.Now()})
			x.jobInd += 1
		}
	case "reduce":
		if len(x.needReduce) == 0 {
			reply.JobType = "idle"
			x.checkUnfinished()
		} else {
			reply.JobType = "reducing"
			reply.JobId = x.jobInd
			upper_bound := int(math.Min(float64(len(x.needReduce)), float64(x.upperbound)))
			reply.ReduceJobs = x.needReduce[:upper_bound]
			x.needReduce = x.needReduce[upper_bound:]
			x.unfinishedReduce = append(x.unfinishedReduce, unfinishedJobs{nil, reply.ReduceJobs, time.Now()})
			x.jobInd += 1
		}
	case "end":
		reply.JobType = "quit"
	default:
		reply.JobType = "idle"
	}
	return nil
}

func removeMap(s *[]unfinishedJobs, thing []string) error {
	if len(*s) == 0 {
		return errors.New("job concidered dead")
	}
	j := 0
	for i, v := range *s {
		if v.MapJobs[0] != thing[0] {
			if i != j {
				(*s)[j] = v
			}
			j += 1
		}
	}
	if len(*s) == j {
		return errors.New("job concidered dead")
	}
	*s = (*s)[:j]
	return nil
}

func removeReduce(s *[]unfinishedJobs, thing [][]string) error {
	if len(*s) == 0 {
		return errors.New("job concidered dead")
	}
	j := 0
	for i, v := range *s {
		if v.ReduceJobs[0][0] != thing[0][0] {
			if i != j {
				(*s)[j] = v
			}
			j += 1
		}
	}
	if len(*s) == j {
		return errors.New("job concidered dead")
	}
	*s = (*s)[:j]
	return nil
}

func (x *Coordinator) FinishJob(args *FinishJobArgs, reply *FinishJobReply) error {
	x.mux.Lock()
	defer x.mux.Unlock()
	x.checkUnfinished()
	switch args.JobType {
	case "mapping":
		err := removeMap(&x.unfinishedMap, args.MapJobs)
		if err != nil {
			log.Print("abandoned job", args)
			reply.Abandoned = true
			break
		}
		reply.Abandoned = false
		x.mapFiles = append(x.mapFiles, args.FileName)
		if x.state == "map" && len(x.needMap) == 0 && len(x.unfinishedMap) == 0 {
			x.state = "reduce"
			intermediate := []KeyValue{}
			files := x.mapFiles
			for _, filename := range files {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))
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
				x.needReduce = append(x.needReduce, append([]string{intermediate[i].Key}, values...))
				i = j
			}
			x.upperbound = int(math.Ceil(float64(len(x.needReduce)) / float64(x.nReduce)))
		}
	case "reducing":
		err := removeReduce(&x.unfinishedReduce, args.ReduceJobs)
		if err != nil {
			log.Print("abandoned job")
			reply.Abandoned = true
			break
		}
		reply.Abandoned = false
		if len(x.needReduce) == 0 && len(x.unfinishedReduce) == 0 {
			x.state = "end"
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	c.mux.Lock()
	c.checkUnfinished()
	ret = len(c.needMap) == 0 && len(c.needReduce) == 0 && c.state == "end"
	c.mux.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.needMap = files
	c.nReduce = nReduce
	c.upperbound = int(math.Ceil(float64(len(c.needMap)) / float64(c.nReduce)))
	c.jobInd = 0
	c.state = "map"

	c.server()
	return &c
}
