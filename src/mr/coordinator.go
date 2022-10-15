package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// 0:mapping, 1:reducing 2:finished
	state     int
	map_tasks []struct {
		string
		int
		time.Time
	}
	intermediate_filenames map[int][]string
	reduce_tasks           []struct {
		int
		time.Time
	}
	mu sync.Mutex
}

// Reply RPCs from workers and send tasks
func (c *Coordinator) Schedule(req RpcEmpty, reply *TaskAssign) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.state {
	case 0:
		all_done := true
		for i, v := range c.map_tasks {
			if v.int == 1 && time.Now().Sub(v.Time).Seconds() > 10 {
				c.map_tasks[i].int = 0
			}
			if v.int == 0 {
				c.map_tasks[i].int = 1
				reply.Filenames = []string{v.string}
				reply.Id = i
				reply.Reduce_num = len(c.reduce_tasks)
				reply.Tasktype = 1
				return nil
			}
			if v.int != 2 {
				all_done = false
			}
		}
		if all_done {
			c.state = 1
		}
	case 1:
		all_done := true
		for i, v := range c.reduce_tasks {
			if v.int == 1 && time.Now().Sub(v.Time).Seconds() > 10 {
				c.reduce_tasks[i].int = 0
			}
			if v.int == 0 {
				c.reduce_tasks[i].int = 1
				reply.Filenames = c.intermediate_filenames[i]
				reply.Id = i
				reply.Reduce_num = len(c.reduce_tasks)
				reply.Tasktype = 2
				return nil
			}
			if v.int != 2 {
				all_done = false
			}
		}
		if all_done {
			c.state = 2
		}
	}

	return nil
}

func (c *Coordinator) Report(req TaskReturn, reply *RpcEmpty) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch req.Tasktype {
	case 1:
		if c.map_tasks[req.Id].int != 2 {
			c.map_tasks[req.Id].int = 2
			for _, filename := range req.Filenames {
				split := strings.Split(filename, "-")
				num, err := strconv.Atoi(split[len(split)-1])
				if err != nil {
					log.Fatalf("cannot parse %v", split[len(split)-1])
				}
				c.intermediate_filenames[num] = append(c.intermediate_filenames[num], filename)
			}
		}
	case 2:
		c.reduce_tasks[req.Id].int = 2
	}
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
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	// fmt.Printf("%v", c.map_tasks)
	// fmt.Printf("%v", c.reduce_tasks)
	return c.state == 2
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	map_tasks := make([]struct {
		string
		int
		time.Time
	}, 0)

	for _, v := range files {
		map_tasks = append(map_tasks, struct {
			string
			int
			time.Time
		}{v, 0, time.Now()})
	}

	c := Coordinator{state: 0, map_tasks: map_tasks, reduce_tasks: make([]struct {
		int
		time.Time
	}, nReduce), intermediate_filenames: make(map[int][]string)}

	c.server()
	return &c
}
