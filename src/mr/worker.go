package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"plugin"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type SlaveState int

const (
	SLAVE_STATE_INIT      = 1
	SLAVE_STATE_CONNECTED = 2
	SLAVE_STATE_LOSS      = 3
)

type Mapper interface {
	MReader(filename string) (string, string)
	Map(key string, value string) []KeyValue
}

type Reducer interface {
	RReader(filename []string) map[string][]string

	Reduce(key string, values []string) string
}

func MapReader(filename string) (string, string) {
	file, err := os.Open(filename)
	if err != nil {
		return filename, ""
	}
	buf := make([]byte, 1024*1024*1024)
	n, err := file.Read(buf)
	if n == 0 || err != nil {
		return filename, ""
	}
	return filename, string(buf)
}

func ReduceReader(filename []string) map[string]([]string) {
	ret := make(map[string]([]string))
	for _, file_name := range filename {
		file, err := os.Open(file_name)
		if err != nil {
			continue
		}
		reader := bufio.NewReader(file)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			kvs := strings.Split(line, " ")
			k := kvs[0]
			v := kvs[1][:len(kvs[1])-1]
			ret[k] = append(ret[k], v)
		}
		file.Close()
	}
	return ret
}

type Slave struct {
	LocalName    string
	state        SlaveState // 可能有多种状态
	MasterIp     string
	MasterPort   int
	ListenPort   int
	MasterConn   *net.Conn
	MasterClient *rpc.Client
	Nreduce      int
	Plugin_file  string
	MapFunc      func(string, string) []KeyValue
	ReduceFunc   func(string, []string) string
}

// 创建函数
func MakeSlave(mip string, mport int, lport int) Slave {
	ret := Slave{}
	ret.state = SLAVE_STATE_INIT
	ret.MasterIp = mip
	ret.MasterPort = mport
	ret.ListenPort = lport
	ret.Nreduce = 10
	return ret
}

func (s *Slave) Listen() error {
	rpc.Register(s)

	l, err := net.Listen("tcp", "127.0.0.1:"+fmt.Sprint(s.ListenPort))
	if err != nil {
		log.Panic("开启RPC监听端口失败")
	} else {
		log.Println("开启RPC服务器成功！")
	}
	rpc.Accept(l)
	return nil
}
func (s *Slave) Connect() {
	addr := fmt.Sprintf("%s:%d", s.MasterIp, s.MasterPort)
	conn, err := net.Dial("tcp", addr)
	writer := bufio.NewWriter(conn)
	s.LocalName = conn.LocalAddr().String()
	s.MasterConn = &conn
	writer.WriteString(fmt.Sprintf("%d\n", s.ListenPort))
	writer.Flush()
	time.Sleep(10e8)
	client := rpc.NewClient(conn)
	if err != nil {
		s.state = SLAVE_STATE_LOSS
		log.Panic("创建RPC连接失败")
	} else {
		log.Println("连接master成功！")
	}
	s.MasterClient = client
	s.state = SLAVE_STATE_CONNECTED

}

func (s *Slave) DoMap(input_file string) ([]string, bool) {
	start := time.Now().Unix()
	log.Printf("开始执行map任务:\n %s", input_file)
	k, v := MapReader(input_file)
	kvs := s.MapFunc(k, v)

	reduce_list := make([]([]KeyValue), s.Nreduce)
	for _, kv := range kvs {
		n := ihash(kv.Key) % s.Nreduce
		reduce_list[n] = append(reduce_list[n], kv)
	}
	var reduce_file_slice []string
	for i := 0; i < s.Nreduce; i++ {
		reduce_file, err := os.Create(fmt.Sprintf("./%d_%s_%s.txt", i, s.LocalName, filepath.Base(input_file)))
		reduce_writer := bufio.NewWriter(reduce_file)
		if err != nil {
			log.Panic("create reduce file falid")
		}
		for _, kv := range reduce_list[i] {
			line := fmt.Sprintf("%s %s\n", kv.Key, kv.Value)
			reduce_writer.WriteString(line)
		}
		reduce_file_slice = append(reduce_file_slice, reduce_file.Name())
		reduce_writer.Flush()
		reduce_file.Close()
	}
	log.Printf("Map任务执行完成! 共耗时：%d", time.Now().Unix()-start)
	return reduce_file_slice, true
}

func (s *Slave) DoReduce(input_files []string) (string, bool) {
	if len(input_files) == 0 {
		return "", true
	}
	kvs := ReduceReader(input_files)
	log.Printf("共输入了%d个key", len(kvs))
	ret := []KeyValue{}
	for key, values := range kvs {
		output := s.ReduceFunc(key, values)
		ret = append(ret, KeyValue{key, output})
	}
	id, _ := strconv.Atoi(strings.Split(filepath.Base(input_files[0]), "_")[0])
	save_file := fmt.Sprintf("./mr-out-%d", id)
	file, err := os.Create(save_file)
	if err != nil {
		return "", false
	}
	writer := bufio.NewWriter(file)
	for _, kv := range ret {
		writer.WriteString(fmt.Sprintf("%s %s\n", kv.Key, kv.Value))
	}
	writer.Flush()
	file.Close()
	return save_file, true
}

func (s *Slave) LoadPlugin() error {
	req := MRPRequest{}
	res := MRPesponse{}
	for s.MasterClient == nil {
		time.Sleep(10e8)
	}
	if s.Plugin_file == "" {
		err := s.MasterClient.Call("Coordinator.GetPluginFile", &req, &res)
		if err != nil {
			log.Panic(err.Error())
		}
		s.Plugin_file = res.Res[0].(string)
	}
	log.Printf("获取到插件文件：%s", s.Plugin_file)

	p, err := plugin.Open(s.Plugin_file)
	if err != nil {
		log.Panic("插件加载失败!")
	}
	mapfunc, err := p.Lookup("Map")
	if err != nil {
		log.Panic("插件未定义Map函数")
	}
	reducefunc, err := p.Lookup("Reduce")
	if err != nil {
		log.Panic("插件未定义Reduce函数")
	}
	s.MapFunc = mapfunc.(func(string, string) []KeyValue)
	s.ReduceFunc = reducefunc.(func(string, []string) string)
	log.Println("插件加载成功!")
	return nil
}

func (s *Slave) Process(req *MRPRequest, res *MRPesponse) error {
	if s.MapFunc == nil {
		s.LoadPlugin()
	}
	task_type := req.Params[1].(string)
	if task_type == "MAP" {
		input_file := req.Params[2].(string)
		file_name, isok := s.DoMap(input_file)
		res.Res = append(res.Res, isok)
		if !isok {
			return fmt.Errorf("Map任务执行失败")
		}
		res.Res = append(res.Res, file_name)

	} else if task_type == "REDUCE" {
		input_files := req.Params[2].([]string)
		file_name, isok := s.DoReduce(input_files)
		res.Res = append(res.Res, isok)
		if !isok {
			return fmt.Errorf("Reduce任务执行失败")
		}
		res.Res = append(res.Res, file_name)
	}

	return nil
}

func (s *Slave) Ping() error {
	for {
		req := MRPRequest{}
		req.Params = append(req.Params, s.LocalName)
		res := MRPesponse{}
		err := s.MasterClient.Call("Coordinator.Ping", &req, &res)
		if err != nil {
			log.Printf("发送心跳失败：%s", err.Error())
			return nil
		}
		log.Printf("发送心跳成功：%d", res.Res[0].(int64))
		time.Sleep(5e9)
	}
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
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
