package main
 
import (
	"fmt"
	"os"
	"bufio"
	"strings"
	"sync"
)

const MAX_COUNT = 10


type ElectionResult struct {
	uik int
	region string
	subregion string
	Baburin int
	Grudinin int
	Jirinovskiy int
	Putin int
	Sobchak int
	Suraikin int
	Titov int
	sheets_in_box_count int
	valid_sheets_count int
	voiters_as_voiters_count int
	sheets_count_in_carring_box int
	sheets_count_given_indoors int
	sheets_count_given_outdoors int
	sheets_count_given_before_election int
	sheets_count_of_not_counted_ones int
	got_by_commission_sheets_count int
	invalid_sheets_count int
	done_sheets_count int
	lost_sheets_count int
}


func main() {
	var data_filename string = "voting_data_rus.csv"
	file, err := os.Open(data_filename)
    if err != nil {
        panic(err)
    }
	scanner := bufio.NewScanner(file)

	if scanner.Scan(){
		line := scanner.Text()
		head := strings.Split(line, ",")

		fmt.Println(head)

		max_read := 10
		count := 0

		var wg sync.WaitGroup
		var mux sync.Mutex
		for scanner.Scan(){
			count++

			wg.Add(1)
			go func(scanner *bufio.Scanner, mux *sync.Mutex){
				defer wg.Done()

				mux.Lock()
				line := scanner.Text()
				parts := strings.Split(line, ",")
				fmt.Println(parts)
				mux.Unlock()
			}(scanner, &mux)

			if count == max_read || !(max_read > 0) {
				break
			}
		}
		wg.Wait()
	}
}