package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	id := strconv.Itoa(os.Getpid()) // 进程 id 作为 worker id
	log.Printf("Worker %d starts working \n", id)

	var lastTaskType string
	var lastTaskIndex int

	for {
		args := ApplyForTaskArgs{
			WorkerID:      id,
			LastTaskType:  lastTaskType,
			LastTaskIndex: lastTaskIndex,
		}

		reply := ApplyForTaskReply{}
		call("Coordinator.ApplyForTask", &args, &reply)

		if reply.TaskType == "" {
			// mr任务完成
			log.Printf("Worker %s has finished all the tasks\n", id)
			break
		}
		log.Printf("Received job %s task %d from coordinator\n", reply.TaskType, reply.TaskIndex)

		if reply.TaskType == MAP {
			// 任务类型为 MAP 任务
			file, err := os.Open(reply.MapInputFile)
			// 打开文件
			if err != nil {
				log.Fatalf("cannot open file: %v", err)
			}
			// 读取文件
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read file: %v", err)
			}
			// 得到中间结果，即一堆 kv
			kva := mapf(reply.MapInputFile, string(content))
			hashedKV := make(map[int][]KeyValue)
			// 分桶, 把
			for _, kv := range kva {
				hashed := ihash(kv.Key) % reply.ReduceNum
				hashedKV[hashed] = append(hashedKV[hashed], kv)
			}
			// 写出中间结果文件
			for i := 0; i < reply.ReduceNum; i++ {
				ofile, _ := os.Create(tmpMapOutFile(id, reply.TaskIndex, i))
				for _, kv := range hashedKV[i] {
					fmt.Fprintf(ofile, "%v\t%v\n", kv.Value, kv.Value)
				}
				ofile.Close()
			}

		} else if reply.TaskType == REDUCE {
			// 任务类型为 REDUCE 任务
			var lines []string
			for mi := 0; mi < reply.ReduceNum; mi++ {
				inputFile := finalMapOutFile(mi, reply.TaskIndex)
				file, err := os.Open(inputFile)
				// 打开文件
				if err != nil {
					log.Fatalf("cannot open file: %v", err)
				}
				// 读取文件
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read file: %v", err)
				}
				lines = append(lines, strings.Split(string(content), "\n")...)
			}
			// 文件中收集到的所有键值对
			var kva []KeyValue
			for _, line := range lines {
				if strings.TrimSpace(line) == "" {
					continue
				}
				parts := strings.Split(line, "\t")
				kva = append(kva, KeyValue{parts[0], parts[1]})
			}
			// 排序，以降重
			sort.Sort(ByKey(kva))
			// 存放结果文件
			ofile, _ := os.Create(tmpReduceOutFile(id, reply.TaskIndex))
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				// 将结果写入文件，每一行的格式为 (key key.count)
				_, err := fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				if err != nil {
					log.Fatalf("cannot write to file: %v", err)
				}
				i = j
			}
			ofile.Close()
		}
		lastTaskIndex = reply.TaskIndex
		lastTaskType = reply.TaskType
		log.Printf("Worker %v has finished the task %v\n", id, reply.TaskIndex)
	}
	log.Printf("Worker %v exit \n", id)

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// example function to show how to make an RPC call to the coordinator.

// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
