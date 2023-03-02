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


type Task struct {
	fileName  string
	id        int
	startTime time.Time
	tasktype  int8   //0: map task, 1: reduce task
	status    int8   //0: waiting, 1: finished
}


type Coordinator struct {
	// Your definitions here.
	Nmap int64
	Nreduce int64
	FileList []string
	Mutex sync.Mutex

	heartbeatCh chan heartbaetMssg
	reportCh    chan reportMsg
	doneCh      chan struct{}
}

type heartbaetMssg struct {
	response    *HeartbeatResponse
	ok          chan struct{}
}

type reportMsg struct {
	request *RePortRequest
	ok      chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) HeartBeat(request *HeartbeatResponse,response *HeartbeatResponse) error {
	msg := heartbaetMssg{response: response,ok: make(chan struct{})}
	c.heartbeatCh <- msg
	<- msg.ok
	return nil
}

func (c *Coordinator) Report(request *RePortRequest, response *RePortResponse) error {
	msg := reportMsg{request: request,ok : make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) schedule() {
	c.initMapPhase()
	for {
		select {
		case msg := <-c.heartbeatCh:
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
			msg.ok <- struct{}{}
		}
	}
}

//准备map任务
func (c *Coordinator) initMapPhase()  {

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
	files = c.FileList
	c.Nmap = int64(len(files))
	c.Nreduce = int64(nReduce)
	c.server()
	return &c
}
