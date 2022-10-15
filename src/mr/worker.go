package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		assign, is_alive := AskTask()
		if !is_alive {
			return
		}
		switch assign.Tasktype {
		case 0:
			time.Sleep(time.Second)
		case 1:
			file, err := os.Open(assign.Filenames[0])
			if err != nil {
				log.Fatalf("cannot open %v", assign.Filenames[0])
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", assign.Filenames[0])
			}
			file.Close()
			kva := mapf(assign.Filenames[0], string(content))
			m := make(map[string][]KeyValue)
			for _, kv := range kva {
				output_filename := "mr-" + strconv.Itoa(assign.Id) + "-" + strconv.Itoa(ihash(kv.Key)%assign.Reduce_num)
				m[output_filename] = append(m[output_filename], kv)
			}
			filenames := make([]string, 0)
			for filename, kvs := range m {
				file, err := os.Create(filename)
				if err != nil {
					log.Fatalf("cannot create %v: %v", filename, err)
				}
				enc := json.NewEncoder(file)
				for _, kv := range kvs {
					err = enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encode kv %v", kv)
					}
				}
				file.Close()
				filenames = append(filenames, filename)
			}
			ret := TaskReturn{Tasktype: 1, Id: assign.Id, Filenames: filenames}
			ReturnTask(ret)
		case 2:
			kva := make(map[string][]string)
			for _, filename := range assign.Filenames {
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
					kva[kv.Key] = append(kva[kv.Key], kv.Value)
				}
				file.Close()
			}
			output_filename := "mr-out-" + strconv.Itoa(assign.Id)
			file, err := os.Create(output_filename)
			if err != nil {
				log.Fatalf("cannot create %v", output_filename)
			}
			for k, vs := range kva {
				output := reducef(k, vs)
				fmt.Fprintf(file, "%v %v\n", k, output)
			}
			file.Close()
			ret := TaskReturn{Tasktype: 2, Id: assign.Id}
			ReturnTask(ret)
		default:
			fmt.Println("unknown rpc call")
		}
	}

}

// Ask coordinator for task to run
func AskTask() (TaskAssign, bool) {
	assign := TaskAssign{}
	ok := call("Coordinator.Schedule", RpcEmpty{}, &assign)
	// if assign.Tasktype != 0 {
	// 	log.Printf("got task %v\n", assign)
	// }
	return assign, ok
}

// Return result to coordinator
func ReturnTask(ret TaskReturn) {
	// log.Printf("return task %v\n", ret)
	call("Coordinator.Report", ret, &RpcEmpty{})
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
