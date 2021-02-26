package core

import (
	"fmt"
	"time"
	"bufio"
	"strconv"
	"github.com/LaCumbancha/yelp-review-ha/cmd/common/utils"

	comms "github.com/LaCumbancha/yelp-review-ha/cmd/common/communication"
)


func LoadDatasetNumber(reader *bufio.Reader, dataset int, processedDatasets []int) int {
	fmt.Printf("Dataset number [default is %d]: ", dataset)
	inputDataset := utils.ReadInput(reader)
	for true {
		if inputDataset == "" {
			return dataset
		} else {
			newDatasetNumber, err := strconv.Atoi(inputDataset)
			if err == nil {
				if utils.IntInSlice(newDatasetNumber, processedDatasets) {
					fmt.Printf("Dataset #%d already processed. Retry: ", newDatasetNumber)
					inputDataset = utils.ReadInput(reader)
				} else {
					return newDatasetNumber
				}
			} else {
				fmt.Print("Wrong value. Must be an integer: ")
				inputDataset = utils.ReadInput(reader)
			}
		}
	}

	return 0
}

func LoadFullReview(reader *bufio.Reader) comms.FullReview {
	inputReview := comms.FullReview{}
	fmt.Println()

	fmt.Print("Review ID: ")
	inputReview.ReviewId = utils.ReadInput(reader)

	fmt.Print("User ID: ")
	inputReview.UserId = utils.ReadInput(reader)

	fmt.Print("Business ID: ")
	inputReview.BusinessId = utils.ReadInput(reader)

	fmt.Print("Stars [numeric]: ")
	inputReview.Stars = utils.ReadInputFloat32(reader)
	
	fmt.Print("Useful [numeric]: ")
	inputReview.Useful = utils.ReadInputInt(reader)

	fmt.Print("Funny [numeric]: ")
	inputReview.Funny = utils.ReadInputInt(reader)

	fmt.Print("Cool [numeric]: ")
	inputReview.Cool = utils.ReadInputInt(reader)

	fmt.Print("Text: ")
	inputReview.Text = utils.ReadInput(reader)

	fmt.Println()
	inputReview.Date = time.Now().Format("2006-01-02 15:04:05")

	return inputReview
}