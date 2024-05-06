package main

import (
	"log"
	"os"
	"strconv"

	"6.5840/mr"
)

func main() {
	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Panic(err.Error())
	}
	slave := mr.MakeSlave("127.0.0.1", 1234, port)
	go slave.Listen()

	slave.Connect()

	slave.Ping()
}
