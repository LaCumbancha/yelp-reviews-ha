package core

import (
	"os"
	"fmt"
	"bufio"
	"strings"
	"encoding/json"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/yelp-review-ha/cmd/common/utils"

	log "github.com/sirupsen/logrus"
	props "github.com/LaCumbancha/yelp-review-ha/cmd/common/properties"
	comms "github.com/LaCumbancha/yelp-review-ha/cmd/common/communication"
	rabbit "github.com/LaCumbancha/yelp-review-ha/cmd/common/middleware"
	proc "github.com/LaCumbancha/yelp-review-ha/cmd/common/processing"
)

type StreamerConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort		  	string
	BulkSize			int
	FunbizMappers	   	int
	WeekdaysMappers	 	int
	HashesMappers	   	int
	UsersMappers		int
	StarsMappers		int
}

type Streamer struct {
	instance 			string
	data				string
	connection		  	*amqp.Connection
	channel			 	*amqp.Channel
	bulkSize			int
	outputFanout		*rabbit.RabbitOutputFanout
	outputSignals	   	int
	processedDatasets   []int
}

func NewStreamer(config StreamerConfig) *Streamer {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	outputFanout := rabbit.NewRabbitOutputFanout(channel, props.InputI2_Output)
	
	streamer := &Streamer {
		instance:			"1",
		connection:		   	connection,
		channel:			channel,
		bulkSize:			config.BulkSize,
		outputFanout:		outputFanout,
		outputSignals:		utils.MaxInt(config.FunbizMappers, config.WeekdaysMappers, config.HashesMappers, config.UsersMappers, config.StarsMappers),
		processedDatasets:	make([]int, 0),
	}

	return streamer
}

func (streamer *Streamer) Run() {
	reader := bufio.NewReader(os.Stdin)
	dataset := proc.DefaultDataset
	exitFlag := false

	for !exitFlag {
		streamer.printMenu()
		fmt.Print("Option: ")
		option := strings.ToUpper(utils.ReadInput(reader))
		message := -1

		for {
			if option == "S" {
				streamer.processReview(reader, &dataset, message)
				break
			} else if option == "X" {
				streamer.closeConnection()
				exitFlag = true
				break
			} else {
				fmt.Print("Wrong option. Retry: ")
				option = utils.ReadInput(reader)
			}
		}
	}
}

func (streamer *Streamer) printMenu() {
	fmt.Println()
	fmt.Println("Reviews Streamer")
	fmt.Println("---------------")
	fmt.Println("[S] STREAM DATASET")
	fmt.Println("[X] CLOSE")
}

func (streamer *Streamer) processReview(reader *bufio.Reader, dataset *int, message int) {
	*dataset++
	*dataset = LoadDatasetNumber(reader, *dataset, streamer.processedDatasets)
	streamer.processedDatasets = append(streamer.processedDatasets, *dataset)

	streamer.startDataset(*dataset)
	datasetFinished := false
	for !datasetFinished {
		review := LoadFullReview(reader)
		streamer.sendReview(*dataset, message, review)

		fmt.Printf("Stream a new review for dataset %d? [Y/N] ", *dataset)
		option := strings.ToUpper(utils.ReadInput(reader))

		for {
			if option == "N" {
				streamer.finishDataset(*dataset)
				datasetFinished = true
				break
			} else if option == "Y" {
				message--
				break
			} else {
				fmt.Print("Wrong option. Retry: ")
				option = utils.ReadInput(reader)
			}
		}
	}
}

func (streamer *Streamer) sendReview(dataset int, number int, review comms.FullReview) {
	bytes, err := json.Marshal(review)
	if err != nil {
		fmt.Errorf("Error generating JSON from review. Error: %s", err)
		return
	}

	err = streamer.outputFanout.PublishData([]byte(comms.SignMessage(props.InputI2_Name, dataset, streamer.instance, number, string(bytes))))
	if err != nil {
		log.Errorf("Error sending message #%d to fanout-exchange %s. Err: '%s'", number, streamer.outputFanout.Exchange, err)
	} else {
		log.Infof("Message #%d sent to fanout-exchange %s.", number, streamer.outputFanout.Exchange)
	}
}

func (streamer *Streamer) startDataset(dataset int) {
	// Sending Start-Message to consumers.
	fmt.Println()
	rabbit.OutputFanoutStart(comms.StartMessageSigned(props.InputI2_Name, dataset, streamer.instance), streamer.outputSignals, streamer.outputFanout)
}

func (streamer *Streamer) finishDataset(dataset int) {
	// Sending Finish-Message to consumers.
	fmt.Println()
	rabbit.OutputFanoutFinish(comms.FinishMessageSigned(props.InputI2_Name, dataset, streamer.instance), streamer.outputSignals, streamer.outputFanout)
}

func (streamer *Streamer) closeConnection() {
	// Sending Close-Message to consumers.
	fmt.Println()
	rabbit.OutputFanoutClose(comms.CloseMessageSigned(props.InputI2_Name, streamer.instance), streamer.outputSignals, streamer.outputFanout)
}

func (streamer *Streamer) Stop() {
	log.Infof("Closing Reviews-Streamer connections.")
	streamer.connection.Close()
	streamer.channel.Close()
}
