package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskPhase int

const (
	TaskStatusReady   = 0 //任务以就绪
	TaskStatusQueue   = 1 //任务正在队列中
	TaskStatusRunning = 2 //任务正在进行
	TaskStatusFinish  = 3 //任务完成
	TaskStatusErr     = 4 //任务出错
)

const (
	MaxTaskRunTime   = time.Second * 5
	ScheduleInterval = time.Millisecond * 500
)

type TaskStat struct {
	Status    int
	WorkerId  int
	StartTime time.Time
}

type Master struct {
	// Your definitions here.
	files     []string
	nReduce   int
	taskPhase TaskPhase  //任务类型
	taskStats []TaskStat //任务状态
	mu        sync.Mutex
	done      bool
	workerSeq int
	taskCh    chan Task
}

// getTask 初始化任务
func (m *Master) getTask(taskSeq int) Task {
	task := Task{
		FileName: "",
		NReduce:  m.nReduce,
		NMaps:    len(m.files),
		Seq:      taskSeq,
		Phase:    m.taskPhase,
		Alive:    true,
	}
	log.Printf("m:%+v, taskseq:%d, lenfiles:%d, lents:%d", m, taskSeq, len(m.files), len(m.taskStats))
	if task.Phase == MapPhase {
		task.FileName = m.files[taskSeq]
	}
	return task
}

// schedule 调度任务
func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.done {
		return
	}
	allFinish := true
	for index, t := range m.taskStats {
		switch t.Status {
		case TaskStatusReady:
			allFinish = false
			m.taskCh <- m.getTask(index)
			m.taskStats[index].Status = TaskStatusQueue
		case TaskStatusQueue:
			allFinish = false
		case TaskStatusRunning:
			allFinish = false
			if time.Now().Sub(t.StartTime) > MaxTaskRunTime { //时间片用完进行调度
				m.taskStats[index].Status = TaskStatusQueue
				m.taskCh <- m.getTask(index)
			}
		case TaskStatusFinish:
		case TaskStatusErr:
			allFinish = false
			m.taskStats[index].Status = TaskStatusQueue
			m.taskCh <- m.getTask(index)
		default:
			log.Fatal("t.status err")
		}
	}
	if allFinish {
		if m.taskPhase == MapPhase {
			m.initReduceTask()
		} else {
			m.done = true
		}
	}
}

func (m *Master) initMapTask() {
	m.taskPhase = MapPhase
	m.taskStats = make([]TaskStat, len(m.files))
}

func (m *Master) initReduceTask() {
	log.Printf("init ReduceTask")
	m.taskPhase = ReducePhase
	m.taskStats = make([]TaskStat, m.nReduce)
}

// regTask 配置任务信息
func (m *Master) regTask(args *TaskArgs, task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if task.Phase != m.taskPhase {
		log.Fatal("req Task phase neq")
	}

	m.taskStats[task.Seq].Status = TaskStatusRunning
	m.taskStats[task.Seq].WorkerId = args.WorkerId
	m.taskStats[task.Seq].StartTime = time.Now()
}

// GiveOneTask 从任务队列中拿出就绪任务进行执行
func (m *Master) GiveOneTask(args *TaskArgs, reply *TaskReply) error {
	task := <-m.taskCh
	reply.Task = &task

	if task.Alive {
		m.regTask(args, &task)
	}
	log.Printf("in get one Task, args:%+v, reply:%+v", args, reply)
	return nil
}

// ReportTask 汇报任务状态
func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Printf("get report task: %+v, taskPhase: %+v", args, m.taskPhase)

	//检查参数
	if m.taskPhase != args.Phase || args.WorkerId != m.taskStats[args.Seq].WorkerId {
		return nil
	}

	if args.Done {
		m.taskStats[args.Seq].Status = TaskStatusFinish
	} else {
		m.taskStats[args.Seq].Status = TaskStatusErr
	}

	go m.schedule()
	return nil
}

// RegWorker 注册worker
func (m *Master) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerSeq += 1
	reply.WorkerId = m.workerSeq
	return nil
}

func (m *Master) server() {
	rpc.Register(m)
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

func (m *Master) tickSchedule() {
	// 应该是每个 task 一个 timer，此处简单处理
	for !m.Done() {
		go m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

// create a Master.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mu = sync.Mutex{}
	m.nReduce = nReduce
	m.files = files
	if nReduce > len(files) {
		m.taskCh = make(chan Task, nReduce)
	} else {
		m.taskCh = make(chan Task, len(m.files))
	}

	m.initMapTask()
	go m.tickSchedule()
	m.server()
	log.Printf("master init")
	// Your code here.
	return &m
}
