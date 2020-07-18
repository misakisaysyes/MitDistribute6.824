package mr

import "log"

func (m *Master)schedule() bool {

	log.Println("_调度分配任务_")

	// if m.nWorker == 0 || (m.nWorker == 1 && m.wkState[m.nWorker - 1] != IDLE) {
	// 	return false
	// }

    wkAddr := <- m.wkChan

	//curWorker := 0
	// m.wkState[curWorker] = RUNNING

	wkSocketAddr := "wk" + wkAddr + "-socket"
	res := call("WorkerElem.DoTask", wkSocketAddr, &Task{"Job1", MAP, 0, m.mapTasks[0], m.nReduce, IDLE}, new(struct{}))
	if !res {
		log.Println("_分配任务失败_", res)
	} else {
		log.Println("_分配任务成功_")
	}

	return true
}