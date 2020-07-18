package mr

import "os"
import "log"
import "plugin"
import "io/ioutil"
import "hash/fnv"
import "strconv"
import "bufio"

type ByKey []KeyValue

type KeyValue struct {
	Key   string
	Value string
}

func DoMap(args *Task){

	log.Println("_调用DoMAP_")

	mapf := mapfPlugLoadin(os.Args[1])	
	intermediate := []KeyValue{}
	
	//1.读入文件，递交给Map函数
	rpath := "../main/" + args.OtherPara
	content, err := ioutil.ReadFile(rpath)
	if	err != nil {
		log.Fatalf("_读文件失败_", rpath)
	} else {
		log.Println("_worker成功读入_",err)
		//log.Println(content)
	}

	kva := mapf(args.OtherPara, string(content))
	intermediate = append(intermediate, kva...)
	//sort.Sort(ByKey(intermediate))	//排序额外工作便于reduce后续处理
	//log.Println(intermediate)

	// 这里一定要使用bufio 否则会出现写串丢失的情况
	x := args.TkNum
	//test := 0
	wFileMap := make(map[string]*bufio.Writer)
	for i := 0; i < len(intermediate); i++ { 
		y := ihash(intermediate[i].Key) % args.MXR
		oPath := "mr-" + strconv.Itoa(x) + "-" +  strconv.Itoa(y) + ".txt"
		wFile, ok := wFileMap[oPath]
		if ok == false {
			oFile, oerr := os.OpenFile(oPath, os.O_WRONLY|os.O_CREATE, 0666)
			if oerr != nil {
				return
			}
			defer oFile.Close()
			
			wFileMap[oPath] = bufio.NewWriter(oFile)
			wFile = wFileMap[oPath]
		}
		wFile.WriteString(intermediate[i].Key + " ")
		wFile.WriteString(intermediate[i].Value + "\n")
		//intermediate[i].Value = strconv.Itoa(test)
		//test = test + 1
		// enc := json.NewEncoder(wFile)
		// enc.Encode(&intermediate[i])
		//log.Println(intermediate[i], strconv.Itoa(R))
	}

	for _, value := range wFileMap {
		value.Flush()
	}

	log.Println("__MAP执行完成__")

	testTask := Task{"Job1",REUDCE,1,"",1, RUNNING}
	DoReduce(&testTask)
}

//将一个map task 分成几个reduce文件
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//从插件中加载Map函数
func mapfPlugLoadin (filename string) (func(string, string) []KeyValue) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("无法加载插件 %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("无法再插件中找到Map函数 %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	return mapf
}
