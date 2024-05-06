package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// 任务类型
const (
	TASK_MAP    = 1
	TASK_REDUCE = 2
)

// 任务状态
const (
	TASK_WAITING = 1
	TASK_PROCESS = 2
	TASK_FINISH  = 2
)

type TaskType int
type TaskState int

type Task struct {
	Task_type   TaskType  // 任务类型
	Task_state  TaskState //任务状态
	Input_file  string    // 任务的输入文件
	Input_files []string  // 任务的输入文件
	slave       string
}

type TimeEvent struct {
	Name        string
	Init_Ticket int
	Ticket      int // 多少ticket执行一次，1 ticket = 0.01s
	CallBack    func()
}

func MakeTimeEvent(init_ticket int, name string, cb func()) TimeEvent {
	te := TimeEvent{}
	te.Init_Ticket = init_ticket
	te.Ticket = init_ticket
	te.Name = name
	te.CallBack = cb
	return te
}

type WorkerSlaveState struct {
	Addr   string
	Ticket int
	Client *rpc.Client
	State  int // 0 空闲，1繁忙，2 断线
}
type Coordinator struct {
	// Your definitions here.
	Tasks_Wait                []Task
	Tasks_Process             []Task
	Tasks_Finish              []Task
	cond_mutex                sync.Mutex
	cond_variable             sync.Cond
	Map_slave                 []Slave
	Reduse_slave              []Slave
	Map_count                 int
	Reducese_count            int
	Task_waiting_queue_mutex  sync.Mutex
	Time_events               []TimeEvent
	WorkersStateMap           map[string]*WorkerSlaveState
	Handle_map_result_chan    chan []string
	Handle_reduce_result_chan chan string
	Slave_ready               chan int
	Plugin_file               string
	Over                      bool
}
type Node struct {
	Name     string `json:"node_name"`
	HostName string `json:"host"`
	Port     int    `json:"port"`
}

type Config struct {
	Nodes []Node `json:"Nodes"`
}

func (c *Coordinator) RegisteTimeEvent(t TimeEvent) {
	c.Time_events = append(c.Time_events, t)
}

// 时间循环函数，用来注册时间事间的
func (c *Coordinator) TimeLoop() {
	log.Printf("开启时钟循环")
	for {
		for i := range c.Time_events {
			c.Time_events[i].Ticket--
			if c.Time_events[i].Ticket == 0 {
				c.Time_events[i].CallBack()
				c.Time_events[i].Ticket = c.Time_events[i].Init_Ticket
			}
		}
		time.Sleep(10e6) // 0.01秒执行一次
	}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.

func (c *Coordinator) Ping(req *MRPRequest, res *MRPesponse) error {
	client_addr := req.Params[0].(string)
	c.WorkersStateMap[client_addr].Ticket = 0
	log.Println("收到心跳：", client_addr)
	unixtime := time.Now().Unix()
	res.Res = append(res.Res, unixtime)
	return nil
}

func (c *Coordinator) GetPluginFile(req *MRPRequest, res *MRPesponse) error {
	if c.Plugin_file == "" {
		return fmt.Errorf("没有指定插件文件")
	}
	res.Res = append(res.Res, c.Plugin_file)

	return nil
}

func LoadConf() Config {
	file, err := os.Open("/Users/regan/code/golang/6.5840/src/mr/node.json")
	if err != nil {
		panic("open config file falid")
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	var cfg Config
	decoder.Decode(&cfg)
	return cfg
}

func (c *Coordinator) do(task Task) {
	var task_type string
	var req MRPRequest
	if task.Task_type == TASK_MAP {
		task_type = "MAP"
		req = MRPRequest{
			Params: []interface{}{task.slave, task_type, task.Input_file, 3},
		}
		log.Printf("分发任务：\n Map, %s", task.Input_file)
	} else {
		task_type = "REDUCE"
		req = MRPRequest{
			Params: []interface{}{task.slave, task_type, task.Input_files, 3},
		}
		log.Printf("分发任务：\n Reduce,%s", strings.Join(task.Input_files, " "))
	}

	res := MRPesponse{}

	err := c.WorkersStateMap[task.slave].Client.Call("Slave.Process", &req, &res)

	if err != nil {
		log.Printf("任务执行失败，将重新加入队列中，失败原因:%s", err.Error())
		task.Task_state = TASK_WAITING
		task.slave = ""
		c.Task_waiting_queue_mutex.Lock()
		c.Tasks_Wait = append(c.Tasks_Wait, task)
		c.Task_waiting_queue_mutex.Unlock()
	} else if task.Task_type == TASK_MAP {
		map_out_file_names := res.Res[1].([]string)
		c.Handle_map_result_chan <- map_out_file_names
		log.Printf("完成任务：Map, %s", filepath.Base(task.Input_file))
	} else if task.Task_type == TASK_REDUCE {
		reduce_output_file_name := res.Res[1].(string)
		c.Handle_reduce_result_chan <- reduce_output_file_name
		log.Printf("完成任务：Reduce, %s", strings.Join(task.Input_files, " "))
	}
	if c.WorkersStateMap[task.slave] != nil {
		c.WorkersStateMap[task.slave].State = 0
		c.cond_variable.Broadcast()
	}

}

func (c *Coordinator) WaitReduce(target int) {
	tasks := make([]Task, c.Reducese_count)
	for i := range tasks {
		tasks[i].Task_state = TASK_WAITING
		tasks[i].Task_type = TASK_REDUCE
	}
	var finish_count int
	for finish_count < target {
		map_out_files := <-c.Handle_map_result_chan
		for i := range map_out_files {
			tasks[i].Input_files = append(tasks[i].Input_files, map_out_files[i])
		}
		finish_count++
	}
	log.Println("开始Reduce")
	c.Task_waiting_queue_mutex.Lock()
	c.Tasks_Wait = append(c.Tasks_Wait, tasks...)
	c.Task_waiting_queue_mutex.Unlock()
	var reduce_finished_count int
	results := []string{}
	for reduce_finished_count < len(tasks) {
		reduce_output_file_name := <-c.Handle_reduce_result_chan
		reduce_finished_count++
		results = append(results, reduce_output_file_name)
	}
	log.Println("任务完成，结果保存:\n", strings.Join(results, "\n"))
	c.Over = true
}

func (c *Coordinator) StartWork() {
	log.Println("开始计算！")
	go c.WaitReduce(len(c.Tasks_Wait))
	for !c.Done() {
		for len(c.Tasks_Wait) > 0 {
			// 如果任务没有分配出去，那么我们需要对其进行分配
			slave_key := c.FetchSlave()
			task := c.Tasks_Wait[0]
			task.Task_state = TASK_PROCESS
			task.slave = slave_key
			c.Task_waiting_queue_mutex.Lock()
			c.Tasks_Wait = c.Tasks_Wait[1:]
			c.Tasks_Process = append(c.Tasks_Process, task)
			c.Task_waiting_queue_mutex.Unlock()
			c.WorkersStateMap[task.slave].State = 1
			go c.do(task)
		}
		time.Sleep(10e7)
	}
}

func (c *Coordinator) FetchSlave() string {
	for {
		for k, s := range c.WorkersStateMap {
			if s.State == 0 {
				return k
			}
		}
		c.cond_variable.Wait()
	}
}

func (c *Coordinator) handle_falid_worker(worker_name string) {
	log.Printf("删除异常连接:%s", worker_name)
	c.WorkersStateMap[worker_name].Client.Close()
	delete(c.WorkersStateMap, worker_name)
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) Server() {
	go c.TimeLoop()

	rpc.Register(c)
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	for {
		client_conn, err := l.Accept()
		if err != nil {
			log.Printf("slave连接失败")
			continue
		}
		p2p_port, _ := bufio.NewReader(client_conn).ReadString('\n')
		log.Printf("slave连接master成功,对端端口为:%s", p2p_port[:len(p2p_port)-1])

		slave := WorkerSlaveState{}
		slave.Addr = client_conn.RemoteAddr().String()
		slave.Ticket = 0
		slave.State = 2
		go rpc.DefaultServer.ServeConn(client_conn)
		slave_client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%s", strings.Split(slave.Addr, ":")[0], p2p_port[:len(p2p_port)-1]))
		if err != nil {
			log.Println("master连接slave失败")
			continue
		} else {
			log.Printf("master连接slave成功")
			slave.Client = slave_client
			slave.State = 0
			c.WorkersStateMap[slave.Addr] = &slave
			c.cond_variable.Broadcast()
		}
	}
}

func (c *Coordinator) Done() bool {
	return c.Over
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, plugin_file string, nReduce int) *Coordinator {
	c := Coordinator{}
	for i := range files {
		task := Task{Task_state: TASK_WAITING, Task_type: TASK_MAP, Input_file: files[i]}
		c.Tasks_Wait = append(c.Tasks_Wait, task)
	}
	c.cond_variable = sync.Cond{L: &c.cond_mutex}
	c.cond_variable.L.Lock()
	c.WorkersStateMap = make(map[string]*WorkerSlaveState)

	check_slave_state_event := MakeTimeEvent(10, "check_slave_state_event", func() {
		for k, v := range c.WorkersStateMap {
			if v.Ticket >= 1000 {
				log.Println("发现超时连接： ", k)
				c.handle_falid_worker(k)
			}
			v.Ticket += 10
		}
	})
	c.RegisteTimeEvent(check_slave_state_event)
	c.Handle_map_result_chan = make(chan []string, 12)
	c.Handle_reduce_result_chan = make(chan string, 12)
	c.Reducese_count = nReduce
	c.Plugin_file = plugin_file
	return &c
}
