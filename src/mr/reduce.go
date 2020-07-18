package mr

import "os"
import "log"
import "plugin"
import "io"
import "strconv"
import "bufio"
import "sort"

func DoReduce(args *Task) {
	log.Println("_调用DoReduce_")
	reducef := reducefPlugLoadin(os.Args[1])	

	var keys []string
	kvs := make(map[string][]string)
	for i := 0; i < args.MXR; i++ {
		rpath := "../main/mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(args.TkNum) + ".txt"
		rFile, rerr := os.Open(rpath)
		if rerr != nil {
			log.Fatalf("_读文件失败_", rpath)
			return
		}
		log.Println("_worker成功读入_",rerr)
		defer rFile.Close()

		rFileHandle := bufio.NewReader(rFile)
		
		for {
			key, kerr := rFileHandle.ReadString(' ')
			value, verr := rFileHandle.ReadString('\n')
			if kerr == io.EOF || verr == io.EOF {
				log.Println("_读入完毕_")
				break
			}
			key = key[:len(key)-1]
			value = value[:len(value)-1]
			_, ok := kvs[key]
			if ok == false {
				keys = append(keys, key)
			}
			kvs[key] = append(kvs[key], value)
			//kvs[key] = kvs[key] + value
			// log.Println("_打印读出的KV_")
			// log.Println(key)
			// log.Println(value)
		}
	}

	// log.Println("_打印kvs_")
	// log.Println(kvs)
	// log.Println("_打印keys_")
	// log.Println(keys)

	sort.Strings(keys)

	oPath := "mr-out-" + strconv.Itoa(args.TkNum) + ".txt"
	oFile, oerr := os.OpenFile(oPath, os.O_WRONLY|os.O_CREATE, 0666)	
	if oerr != nil {
		log.Fatalf("_读文件失败_", oPath)
	 	return
	}
	defer oFile.Close()

	wFile := bufio.NewWriter(oFile)
	for _, value := range keys {
		output := reducef(value, kvs[value])
		wFile.WriteString(value + " ")
		wFile.WriteString(output + "\n")
	}
	wFile.Flush()
}


//从插件中加载Reduce函数
func reducefPlugLoadin (filename string) (func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("无法加载插件 %v", filename)
	}
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("无法再插件中找到Map函数 %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)
	return reducef
}