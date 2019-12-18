package map_reduce

import (
	"../election"
	"encoding/csv"
	"fmt"
	"os"
	"runtime"
	"strconv"
)

type ResultType map[string]election.ElectionRecord

const FileName = "voting_data_rus.csv"

func handleError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func strToInt(str string) int {
	number, err := strconv.Atoi(str)
	if err != nil {
		fmt.Println(err)
		return 0
	}
	return number
}

func MapReduce(chunksCount int, chunkSize int, parallel bool) int {
	cores := runtime.NumCPU()
	fmt.Printf("This machine has %d CPU cores. \n", cores)
	runtime.GOMAXPROCS(cores)

	csvFile, err := os.Open(FileName)
	handleError(err)
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	reader.FieldsPerRecord = -1

	_, err = reader.Read()
	handleError(err)

	var records [][]election.ElectionRecord

	for c := 0; c < cores; c++ {
		records = append(records, []election.ElectionRecord{})
	}

	var results []ResultType
	resultChan := make(chan ResultType)
	linearResults := []ResultType{}

	chunkSize = chunkSize
	rowCounter := 0
	var chunk []election.ElectionRecord

	go func() {
		if parallel {
			left := chunksCount
			for {
				results = append(results, <-resultChan)
				left--
				if left == 0 {
					break
				}
			}
			SumResults(results)
		}
	}()

	for {
		rawCsvRecord, err := reader.Read()
		if err != nil {
		} else {
			rowCounter++
			electionRecord := election.ElectionRecord{
				Region:     rawCsvRecord[1],
				Baburin:    strToInt(rawCsvRecord[3]),
				Grudinin:   strToInt(rawCsvRecord[4]),
				Jirinovsky: strToInt(rawCsvRecord[5]),
				Putin:      strToInt(rawCsvRecord[6]),
				Sobchak:    strToInt(rawCsvRecord[7]),
				Suraykin:   strToInt(rawCsvRecord[8]),
				Titov:      strToInt(rawCsvRecord[9]),
				Yavlinskiy: strToInt(rawCsvRecord[22]),
			}
			chunk = append(chunk, electionRecord)
		}

		if rowCounter == chunkSize || err != nil {
			rowCounter = 0
			chunksCount++

			if parallel {
				go func(resultChanIn chan ResultType, chunkIn []election.ElectionRecord) {
					mapped := Map(chunkIn)
					shuffled := Shuffle(mapped)
					resultChanIn <- Reduce(shuffled)
				}(resultChan, chunk)
			} else {
				mapped := Map(chunk)
				shuffled := Shuffle(mapped)
				linearResults = append(linearResults, Reduce(shuffled))
			}

			chunk = []election.ElectionRecord{}
		}

		if err != nil {
			break
		}
	}

	if !parallel {
		results = linearResults
		SumResults(results)
	}

	return chunksCount
}

func Map(records []election.ElectionRecord) []election.ElectionRecord {
	return records
}

func Shuffle(records []election.ElectionRecord) map[string][]election.ElectionRecord {
	mappedRecords := make(map[string][]election.ElectionRecord)
	for _, record := range records {
		mappedRecords[record.Region] = append(mappedRecords[record.Region], record)
	}

	return mappedRecords
}

func Reduce(mappedRecords map[string][]election.ElectionRecord) ResultType {
	result := make(map[string]election.ElectionRecord)

	for region, electionRecords := range mappedRecords {
		regionResult := election.ElectionRecord{
			Baburin:    0,
			Grudinin:   0,
			Jirinovsky: 0,
			Putin:      0,
			Sobchak:    0,
			Suraykin:   0,
			Titov:      0,
			Yavlinskiy: 0,
		}

		for _, electionRecord := range electionRecords {
			regionResult.Baburin += electionRecord.Baburin
			regionResult.Grudinin += electionRecord.Grudinin
			regionResult.Jirinovsky += electionRecord.Jirinovsky
			regionResult.Putin += electionRecord.Putin
			regionResult.Sobchak += electionRecord.Sobchak
			regionResult.Suraykin += electionRecord.Suraykin
			regionResult.Titov += electionRecord.Titov
			regionResult.Yavlinskiy += electionRecord.Yavlinskiy
		}

		result[region] = regionResult
	}

	return result
}

func SumResults(results []ResultType) {

	finalResult := results[0]

	for _, result := range results[1:] {
		for region, electionResult := range result {
			regionResult := finalResult[region]

			regionResult.Baburin += electionResult.Baburin
			regionResult.Grudinin += electionResult.Grudinin
			regionResult.Jirinovsky += electionResult.Jirinovsky
			regionResult.Putin += electionResult.Putin
			regionResult.Sobchak += electionResult.Sobchak
			regionResult.Suraykin += electionResult.Suraykin
			regionResult.Titov += electionResult.Titov
			regionResult.Yavlinskiy += electionResult.Yavlinskiy

			finalResult[region] = regionResult
		}
	}

	fmt.Println(finalResult["Калининградская область"])
}
