package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Master struct {
	sync.Mutex

	// 作业相关
	jobName string	   // 当前正在执行的作业名
	mapTasks []string  // 一个作业中包含的文件名（每个文件都是一个子任务）
	nReduce int		   // 当前设定的reduce任务数量 

	// worker相关
	wkAddrCond *sync.Cond  // 条件锁，保证register和管道注册执行的同步关系
	wkAddr []string    // 记录每个worker的通信地址
	wkChan chan string // worker调度管道
					   // 新worker注册添加到该管道中
	                   // master分配任务时从中取一个worker
	                   // worker执行任务结束后重新加入管道
	// tasks	[]Task
}

// main/mrmaster.go 调用该函数创建master节点
func MakeMaster(files []string, nReduce int) *Master {
	
	// 初始化master节点
	// 将一个作业划分成多个map任务
	m := Master{jobName:"Job1", mapTasks:split(&files), nReduce:nReduce, wkChan:make(chan string, 10)}
	
	// 启动master节点
	// 监听来自worker的请求
	m.runMaster() 
	
	return &m
}

// main/mrmaster.go 在循环内调用该函数以查看整个作业是否已完成
func (m *Master) Done() bool {
	ret := false

	// 先处理调度map任务
	ret = m.schedule()

	// 再处理调度reduce任务
	// ret = m.schedule()

	return ret
}

// worker远程调用该函数注册其通信地址
func (m *Master) Register(args *string, reply *struct{}) error {
	m.Lock()
	defer m.Unlock()

	m.wkAddr = append(m.wkAddr, *args)
	
	m.wkChan <- *args

	log.Println("_master/register 注册完毕_")
	return nil
}

// 监听来自worker的请求
func (m *Master) runMaster() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("_master/runMaster 打开监听错误_", e)
	}
	go http.Serve(l, nil)
}
