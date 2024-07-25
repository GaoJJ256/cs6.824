package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	lock           sync.Mutex // 保护共享信息，避免并发冲突
	stage          string     // 当前作业阶段，MAP or REDUCE 为空代表已完成可退出
	nMap           int
	nReduce        int
	tasks          map[string]Task
	availableTasks chan Task
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.stage == ""
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		stage:          MAP,
		nMap:           len(files),
		nReduce:        nReduce,
		tasks:          make(map[string]Task),
		availableTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}
	for i, file := range files {
		task := Task{
			Type:         MAP,
			Index:        i,
			MapInputFile: file,
		}
		c.tasks[GetTaskID(task.Type, task.Index)] = task
		c.availableTasks <- task
	}
	c.server() // 服务器启动应在循环外部

	// 启动 Task 自动回收
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.lock.Lock()
			for _, task := range c.tasks {
				if task.WorkerID != "" && time.Now().After(task.Deadline) {
					log.Printf("task %s No.%v is dead on worker %s", task.Type, task.Index, task.WorkerID)
					// 将超时的 task 重新放回 channel，并将 workerID 设为空
					task.WorkerID = ""
					task.Deadline = time.Time{} // 清除任务的截止时间
					c.availableTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()

	return &c
}

// 阶段转换，在所有的 MAP 任务执行完毕或者所有的 REDUCE 任务执行完毕时执行
func (c *Coordinator) cutover() {
	switch c.stage {
	case MAP:
		log.Printf("cutover Map")
		c.stage = REDUCE
		// 生成 Reduce tasks
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				Type:  REDUCE,
				Index: i,
			}
			c.tasks[GetTaskID(task.Type, task.Index)] = task
			c.availableTasks <- task
		}
	case REDUCE:
		log.Printf("cutover Reduce")
		close(c.availableTasks)
		c.stage = ""
	}
}

// 将 task 的类型和索引进行拼接
func GetTaskID(t string, index int) string {
	return fmt.Sprintf("%s-%d", t, index)
}

// worker 向 Coordinator 申请新的 Task 处理函数
func (c *Coordinator) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	// 记录上一个 Task 已经完成
	if args.LastTaskType != "" {
		c.lock.Lock()
		lastTaskId := GetTaskID(args.LastTaskType, args.LastTaskIndex)
		if task, exists := c.tasks[lastTaskId]; exists && task.WorkerID == args.WorkerID {
			log.Printf("Mark %s task %s as finished on worker %s\n",
				task.Type, task.Index, task.WorkerID)

			if args.LastTaskType == MAP {
				for ri := 0; ri < c.nReduce; ri++ {
					err := os.Rename(
						tmpMapOutFile(args.WorkerID, args.LastTaskIndex, ri),
						finalMapOutFile(args.LastTaskIndex, ri))
					if err != nil {
						log.Fatal("fail to rename map output file")
					}
				}
			} else if args.LastTaskType == REDUCE {
				err := os.Rename(
					tmpReduceOutFile(args.WorkerID, args.LastTaskIndex),
					finalReduceOutFile(args.LastTaskIndex))
				if err != nil {
					log.Fatal("fail to rename reduce output file")
				}
			}
			delete(c.tasks, lastTaskId)

			if len(c.tasks) == 0 {
				c.cutover()
			}
		}
		c.lock.Unlock()
	}

	// 获取 task
	task, ok := <-c.availableTasks
	if !ok {
		return nil
	}
	c.lock.Lock()
	defer c.lock.Unlock()

	task.WorkerID = args.WorkerID
	task.Deadline = time.Now().Add(10 * time.Second)
	c.tasks[GetTaskID(task.Type, task.Index)] = task
	reply.TaskType = task.Type
	reply.TaskIndex = task.Index
	reply.MapInputFile = task.MapInputFile
	reply.MapNum = c.nMap
	reply.ReduceNum = c.nReduce
	return nil
}
