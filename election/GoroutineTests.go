package election

import "fmt"

func GoroutineTest() {
	numbers := [...]int{1, 2, 3, 4, 5}

	intChan := make(chan int)

	for _, number := range numbers {
		go func(number int) {
			fmt.Printf("Number is %d\n", number)
			intChan <- number
		}(number)
	}

	left := 5
	for {
		<-intChan
		left--
		if left == 0 {
			break
		}
	}
}
