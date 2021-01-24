package core

import (
	"fmt"
	"sync"
	"strings"
	"encoding/json"
	"github.com/streadway/amqp"
	
	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

type MapperConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	ReviewsInputs		int
	StarsFilters		int
}

type Mapper struct {
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	inputDirect 		*rabbit.RabbitInputDirect
	outputQueue 		*rabbit.RabbitOutputQueue
	endSignals 			int
}

func NewMapper(config MapperConfig) *Mapper {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.ReviewsScatterOutput, props.StarsMapperTopic, props.StarsMapperInput)
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.StarsMapperOutput, comms.EndMessage(config.Instance), comms.EndSignals(config.StarsFilters))

	mapper := &Mapper {
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		inputDirect:		inputDirect,
		outputQueue:		outputQueue,
		endSignals:			config.ReviewsInputs,
	}

	return mapper
}

func (mapper *Mapper) Run() {
	log.Infof("Starting to listen for reviews.")
	innerChannel := make(chan amqp.Delivery)

	closingConn := false
	connMutex := &sync.Mutex{}

	var connWg sync.WaitGroup
	connWg.Add(1)

	var procWg sync.WaitGroup
	procWg.Add(1)

	go proc.InitializeProcessingWorkers(mapper.workersPool, innerChannel, mapper.callback, &procWg)
	go proc.ProcessInputs(mapper.inputDirect.ConsumeData(), innerChannel, mapper.endSignals, &procWg, &connWg)
	go proc.ProcessFinish(mapper.finishCallback, &procWg, closingConn, connMutex)
	proc.CloseConnection(mapper.closeCallback, &procWg, &connWg, closingConn, connMutex)
}

func (mapper *Mapper) callback(bulkNumber int, bulk string) {
	mappedData := mapper.mapData(bulkNumber, bulk)
	mapper.sendMappedData(bulkNumber, mappedData)
}

func (mapper *Mapper) finishCallback() {
	mapper.outputQueue.PublishFinish()
}

func (mapper *Mapper) closeCallback() {
	// TODO
}

func (mapper *Mapper) mapData(bulkNumber int, rawReviewsBulk string) []comms.StarsData {
	var review comms.FullReview
	var starsDataList []comms.StarsData

	rawReviews := strings.Split(rawReviewsBulk, "\n")
	for _, rawReview := range rawReviews {
		json.Unmarshal([]byte(rawReview), &review)
	
		mappedReview := comms.StarsData {
			UserId:		review.UserId,
			Stars:		review.Stars,
		}

		starsDataList = append(starsDataList, mappedReview)
	}

	return starsDataList
}

func (mapper *Mapper) sendMappedData(bulkNumber int, mappedBulk []comms.StarsData) {
	data, err := json.Marshal(mappedBulk)
	if err != nil {
		log.Errorf("Error generating Json from mapped bulk #%d. Err: '%s'", bulkNumber, err)
	} else {
		err := mapper.outputQueue.PublishData(data)

		if err != nil {
			log.Errorf("Error sending mapped bulk #%d to output queue %s. Err: '%s'", bulkNumber, mapper.outputQueue.Name, err)
		} else {
			logb.Instance().Infof(fmt.Sprintf("Mapped bulk #%d sent to output queue %s.", bulkNumber, mapper.outputQueue.Name), bulkNumber)
		}
	}
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing Stars Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}