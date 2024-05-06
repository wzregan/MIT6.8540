package main

import (
	"log"
	"os"
	"strings"

	"6.5840/mr"
)

func main() {
	var plugin_file string
	var input_files []string
	if strings.HasSuffix(os.Args[1], "so") {
		plugin_file = os.Args[1]
		input_files = os.Args[2:]
	} else {
		plugin_file = ""
		input_files = os.Args[1:]
	}
	log.Printf("等待处理的文件有:\n %s", strings.Join(os.Args[1:], "\n  "))
	coor := mr.MakeCoordinator(input_files, plugin_file, 10)

	// 启动监听服务器
	go coor.Server()
	coor.StartWork()
}
