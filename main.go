package main

import (
	"./map_reduce"
	"fmt"
	"time"
)

const recordsCount = 95_000

const chunksCount = 4
const chunkSize = int(recordsCount / chunksCount)

func test(chunksCount int) time.Duration {
	start := time.Now()

	map_reduce.MapReduce(chunksCount, chunkSize, false)

	t := time.Now()
	elapsed := t.Sub(start)
	return elapsed
}

func main() {
	fmt.Println(test(4).Milliseconds())
}
