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

const NODE_CODE = "M2"

type MapperConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	ReviewsInputs		int
	FunbizFilters 		int
}

type Mapper struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	inputDirect 		*rabbit.RabbitInputDirect
	outputQueue 		*rabbit.RabbitOutputQueue
	endSignals			int
}

func NewMapper(config MapperConfig) *Mapper {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.ReviewsScatterOutput, props.FunbizMapperTopic, props.FunbizMapperInput)
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.FunbizMapperOutput, comms.EndSignals(config.FunbizFilters))

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
		mapper.startCallback,
		mapper.finishCallback,
		mapper.closeCallback,
	)
}

func (mapper *Mapper) mainCallback(inputNode string, dataset int, instance string, bulk int, data string) {
	mappedData := mapper.mapData(data)
	mapper.sendMappedData(dataset, bulk, mappedData)
}

func (mapper *Mapper) startCallback(dataset int) {
	// Sending Start-Message to consumers.
	rabbit.OutputQueueStart(comms.StartMessageSigned(NODE_CODE, dataset, mapper.instance), mapper.outputQueue)
}

func (mapper *Mapper) finishCallback(dataset int) {
	// Sending Finish-Message to consumers.
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(NODE_CODE, dataset, mapper.instance), mapper.outputQueue)
}

func (mapper *Mapper) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputQueueClose(comms.CloseMessageSigned(NODE_CODE, mapper.instance), mapper.outputQueue)
}

func (mapper *Mapper) mapData(rawReviewsBulk string) []comms.FunnyBusinessData {
	var review comms.FullReview
	var funbizDataList []comms.FunnyBusinessData

	rawReviews := strings.Split(rawReviewsBulk, "\n")
	for _, rawReview := range rawReviews {
		if rawReview != "" {
			json.Unmarshal([]byte(rawReview), &review)

			mappedReview := comms.FunnyBusinessData {
				BusinessId:		review.BusinessId,
				Funny:			review.Funny,
			}

			funbizDataList = append(funbizDataList, mappedReview)
		} else {
			log.Warnf("Empty RawReview.")
		}
	}

	return funbizDataList
}

func (mapper *Mapper) sendMappedData(dataset int, bulk int, mappedData []comms.FunnyBusinessData) {
	bytes, err := json.Marshal(mappedData)
	if err != nil {
		log.Errorf("Error generating Json from mapped bulk #%d.%d. Err: '%s'", dataset, bulk, err)
	} else {
		data := comms.SignMessage(NODE_CODE, dataset, mapper.instance, bulk, string(bytes))
		err := mapper.outputQueue.PublishData([]byte(data))

		if err != nil {
			log.Errorf("Error sending mapped bulk #%d.%d to output queue %s. Err: '%s'", dataset, bulk, mapper.outputQueue.Name, err)
		} else {
			logb.Instance().Infof(fmt.Sprintf("Mapped bulk #%d.%d sent to output queue %s.", dataset, bulk, mapper.outputQueue.Name), bulk)
		}
	}
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing Funny-Business Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
