package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.run()

}

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// run worker控制
func (w *worker) run() {
	for {
		t := w.reqTask()
		if !t.Alive {
			log.Printf("worker get task not alive, exit")
			return
		}
		w.doTask(t)
	}
}

// reqTask 请求任务
func (w *worker) reqTask() Task {
	args := TaskArgs{}
	args.WorkerId = w.id
	reply := TaskReply{}

	if ok := call("Master.GiveOneTask", &args, &reply); !ok {
		log.Printf("worker get task fail,exit")
		os.Exit(1)
	}
	log.Printf("worker get task:%+v", reply.Task)
	return *reply.Task
}

// doTask 执行任务
func (w *worker) doTask(t Task) {
	log.Printf("in do Task")

	switch t.Phase {
	case MapPhase:
		w.doMapTask(t)
	case ReducePhase:
		w.doReduceTask(t)
	default:
		log.Fatal(fmt.Sprintf("task phase err: %v", t.Phase))
	}

}

// doMapTask 执行map任务
func (w *worker) doMapTask(t Task) {
	file, err := os.Open(t.FileName)
	if err != nil {
		w.reportTask(t, false, err)
		return
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		w.reportTask(t, false, err)
		return
	}
	buffer := make([]byte, fileInfo.Size())
	_, err = file.Read(buffer)
	if err != nil {
		w.reportTask(t, false, err)
		return
	}
	kvs := w.mapf(t.FileName, string(buffer))
	reduces := make([][]KeyValue, t.NReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % t.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, l := range reduces {
		fileName := reduceName(t.Seq, idx)
		f, err := os.Create(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(t, false, err)
			}
		}
		if err := f.Close(); err != nil {
			w.reportTask(t, false, err)
		}
	}
	w.reportTask(t, true, nil)
}

// doReduceTask 执行reduce任务
func (w *worker) doReduceTask(t Task) {
	maps := make(map[string][]string)
	for idx := 0; idx < t.NMaps; idx++ {
		fileName := reduceName(idx, t.Seq)
		file, err := os.Open(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	if err := os.WriteFile(mergeName(t.Seq), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, false, err)
	}

	w.reportTask(t, true, nil)
}

// reportTask  回复完成任务
func (w *worker) reportTask(t Task, done bool, err error) {
	if err != nil {
		log.Printf("%v", err)
	}
	args := ReportTaskArgs{}
	args.Done = done
	args.Seq = t.Seq
	args.Phase = t.Phase
	args.WorkerId = w.id
	reply := ReportTaskReply{}
	if ok := call("Master.ReportTask", &args, &reply); !ok {
		log.Printf("report task fail:%+v", args)
	}
}

// register 向master发起注册
func (w *worker) register() {
	args := &RegisterArgs{}
	reply := &RegisterReply{}
	if ok := call("Master.RegWorker", args, reply); !ok {
		log.Fatal("reg fail")
	}
	w.id = reply.WorkerId
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Printf("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Printf("%+v", err)
	return false
}
