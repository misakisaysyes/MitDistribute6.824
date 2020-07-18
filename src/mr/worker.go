package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
// import "strconv"
import "math/rand"
import "time"
import "sync"

type WorkerElem struct {
	sync.Mutex

	name 	string
	wkNum	int
	wkState state // IDLE worker正闲着，可以接受任务
				  // RUNNING worker正在执行任务
				  // DEAD  worker宕机
				  // SHUTDOWN worker完成任务
	TaskCh	chan  Task // 接收来自master的任务 // mutex
}

// main/mrworker.go 调用这个函数
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// 初始化一个worker	
	wk := WorkerElem{}

	// 启动worker
	wk.runWorker()
}

// 启动worker节点
// 向master注册自己的socket地址
// 监听来自master的命令
func (wk *WorkerElem) runWorker() {
	
	// 初始化worker参数
	wk.name = getRandomStr(4)
	wk.TaskCh = make(chan Task, 1)

	// 创建socket通信管道
	rpc.Register(wk)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":2333")
	url := "wk" + wk.name + "-socket"
	os.Remove(url)
	l, e := net.Listen("unix", url)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Println("_打开监听成功_", url)
	go http.Serve(l, nil)

	// 向master注册并拿到工号回执
	res := call("Master.Register", "mr-socket", wk.name, new(struct{}))
	if !res {
		log.Println("_执行失败_", res)
	} else {
		log.Println("_执行成功_")
		log.Println(wk)
	}
	
	// ret := <-wk.TaskCh
	// log.Println("_成功接受任务_", ret)

	for {
		conn, e := l.Accept()
		if e != nil {
			log.Fatal("_连接错误_:", e)
			continue
		}
		if wk.wkState == DEAD {
			break
		} else {
			log.Println("_监听_")
			conn.Close()
		}
	}
}

// master远程调用该函数给worker分配任务
func (wk *WorkerElem) DoTask(args *Task, reply *struct{}) error {
	// wk.TaskCh<-*args
	
	log.Println("_worker监听任务成功_", *args)
	// log.Println("_工号测试_", wk.wkNum)
	
	wk.Lock()
	if args.TkType == MAP {
		go DoMap(args)
	} else {
		// DoReduce(args)
	}
	wk.Unlock()

	return nil
}

// 随机生成字符串
func  getRandomStr(l int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes := []byte(str)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < l; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}