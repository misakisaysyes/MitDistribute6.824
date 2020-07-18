package mr

import "fmt"
import "log"
import "net/rpc"
//
// RPC definitions.
//
// remember to capitalize all names.
//

type state int

const IDLE = 0
const RUNNING = 1
const DEAD = 2
const SHUTDOWN =3

const MAP = 0
const REUDCE = 1

type Task struct {
	JobName	string
	TkType	int
	TkNum	int
	OtherPara string //map任务此处为文件名
	MXR		int	//执行map任务时该字段代表被分为R个reduce任务
				//执行reduce任务时该字段代表M个map任务
				//worker用此字段查找中间文件
	TkState	int
}


// Add your RPC definitions here.

func call(rpcname string, socketAddr string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1:"+ port)
	c, err := rpc.DialHTTP("unix", socketAddr)
	if err != nil {
		log.Fatal("_拨号错误_:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}