package core

import (
	"fmt"
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
	instance 			string
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
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.StarsMapperOutput, comms.EndSignals(config.StarsFilters))

	mapper := &Mapper {
		instance:			config.Instance,
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
	proc.Transformation(
		mapper.workersPool,
		mapper.endSignals,
		mapper.inputDirect.ConsumeData(),
		mapper.mainCallback,
		mapper.finishCallback,
		mapper.closeCallback,
	)
}

func (mapper *Mapper) mainCallback(datasetNumber int, bulkNumber int, bulk string) {
	mappedData := mapper.mapData(bulk)
	mapper.sendMappedData(datasetNumber, bulkNumber, mappedData)
}

func (mapper *Mapper) finishCallback(datasetNumber int) {
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(datasetNumber, mapper.instance), mapper.outputQueue)
}

func (mapper *Mapper) closeCallback() {
	// TODO
}

func (mapper *Mapper) mapData(rawReviewsBulk string) []comms.StarsData {
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

func (mapper *Mapper) sendMappedData(datasetNumber int, bulkNumber int, mappedBulk []comms.StarsData) {
	bytes, err := json.Marshal(mappedBulk)
	if err != nil {
		log.Errorf("Error generating Json from mapped bulk #%d. Err: '%s'", bulkNumber, err)
	} else {
		data := comms.SignMessage(datasetNumber, mapper.instance, bulkNumber, string(bytes))
		err := mapper.outputQueue.PublishData([]byte(data))

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
