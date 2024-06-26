package main

import (
	"math/rand"
	"os"

	"6.5840/mr"
)

func main() {
	port := (rand.Intn(65535) + 5000) % 65535
	slave := mr.MakeSlave("127.0.0.1", 1234, port)
	slave.Plugin_file = os.Args[1]
	go slave.Listen()

	slave.Connect()

	slave.Ping()
}
