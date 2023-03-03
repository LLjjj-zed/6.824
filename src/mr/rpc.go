package mr

import (
	"fmt"
	"os"
)
import "strconv"

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type Task struct {
	FileName string
	NReduce  int
	NMaps    int
	Seq      int //任务编号
	Phase    TaskPhase
	Alive    bool // 任务存活状态，false时worker退出
}

func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskArgs struct {
	WorkerId int
}

type TaskReply struct {
	Task *Task
}

type ReportTaskArgs struct {
	Done     bool
	Seq      int
	Phase    TaskPhase
	WorkerId int
}

type ReportTaskReply struct {
}

type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerId int
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
