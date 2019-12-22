package main

import (
	"./map_reduce"
	"time"
)

const recordsCount = 95_000
const parallel = true
const chunksCount = 100
const chunkSize = int(recordsCount / chunksCount)
const regionName = "Калининградская область"

func test() time.Duration {
	start := time.Now()

	map_reduce.MapReduce(chunksCount, chunkSize, parallel, regionName)

	t := time.Now()
	elapsed := t.Sub(start)
	return elapsed
}

func main() {
	const attempts = 2
	sumTime := time.Duration(0)
	for i := 0; i < attempts; i++ {
		sumTime += test()
	}
	//fmt.Println("Avg time is ", sumTime.Milliseconds()/attempts)
}
