package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//

import "../mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}

	// ii:=0 
	m := mr.MakeMaster(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
		// fmt.Println("这是第%v阶段\n",ii)
		// ii = ii + 1
	}
	time.Sleep(time.Second)
}
