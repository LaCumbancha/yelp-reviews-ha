package core

import (
	"fmt"
	"strings"
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/yelp-review-ha/cmd/common/logger"
	proc "github.com/LaCumbancha/yelp-review-ha/cmd/common/processing"
	props "github.com/LaCumbancha/yelp-review-ha/cmd/common/properties"
	comms "github.com/LaCumbancha/yelp-review-ha/cmd/common/communication"
	rabbit "github.com/LaCumbancha/yelp-review-ha/cmd/common/middleware"
)

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
	inputFanout 		*rabbit.RabbitInputFanout
	outputQueue 		*rabbit.RabbitOutputQueue
	endSignalsNeeded	map[string]int
}

func NewMapper(config MapperConfig) *Mapper {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputFanout := rabbit.NewRabbitInputFanout(channel, props.InputI2_Output, props.MapperM2_Input)
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.MapperM2_Output, config.FunbizFilters)

	endSignalsNeeded := map[string]int{props.InputI2_Name: config.ReviewsInputs}

	mapper := &Mapper {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		inputFanout:		inputFanout,
		outputQueue:		outputQueue,
		endSignalsNeeded:	endSignalsNeeded,
	}

	return mapper
}

func (mapper *Mapper) Run() {
	log.Infof("Starting to listen for reviews.")
	dataByInput := map[string]<-chan amqp.Delivery{props.InputI2_Name: mapper.inputFanout.ConsumeData()}
	mainCallbackByInput := map[string]func(string, int, string, int, string){props.InputI2_Name: mapper.mainCallback}
	
	proc.ProcessInputsStatelessly(
		dataByInput,
		mapper.workersPool,
		mapper.endSignalsNeeded,
		[]string{},
		mainCallbackByInput,
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
	rabbit.OutputQueueStart(comms.StartMessageSigned(props.MapperM2_Name, dataset, mapper.instance), mapper.outputQueue)
}

func (mapper *Mapper) finishCallback(dataset int) {
	// Sending Finish-Message to consumers.
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(props.MapperM2_Name, dataset, mapper.instance), mapper.outputQueue)
}

func (mapper *Mapper) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputQueueClose(comms.CloseMessageSigned(props.MapperM2_Name, mapper.instance), mapper.outputQueue)
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
		data := comms.SignMessage(props.MapperM2_Name, dataset, mapper.instance, bulk, string(bytes))
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
