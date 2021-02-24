package core

import (
	"fmt"
	"strings"
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/yelp-review-ha/cmd/common/logger"
	utils "github.com/LaCumbancha/yelp-review-ha/cmd/common/utils"
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
	UserAggregators		int
}

type Mapper struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	inputFanout 		*rabbit.RabbitInputFanout
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignalsNeeded	map[string]int
}

func NewMapper(config MapperConfig) *Mapper {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputFanout := rabbit.NewRabbitInputFanout(channel, props.InputI2_Output, props.MapperM5_Input)
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.MapperM5_Output)

	endSignalsNeeded := map[string]int{props.InputI2_Name: config.ReviewsInputs}

	mapper := &Mapper {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		inputFanout:		inputFanout,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.UserAggregators, PartitionableValues),
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
	rabbit.OutputDirectStart(comms.StartMessageSigned(props.MapperM5_Name, dataset, mapper.instance), mapper.outputPartitions, mapper.outputDirect)
}

func (mapper *Mapper) finishCallback(dataset int) {
	// Sending Finish-Message to consumers.
	rabbit.OutputDirectFinish(comms.FinishMessageSigned(props.MapperM5_Name, dataset, mapper.instance), mapper.outputPartitions, mapper.outputDirect)
}

func (mapper *Mapper) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputDirectClose(comms.CloseMessageSigned(props.MapperM5_Name, mapper.instance), mapper.outputPartitions, mapper.outputDirect)
}

func (mapper *Mapper) mapData(rawReviewsBulk string) []comms.UserData {
	var review comms.FullReview
	var userDataList []comms.UserData

	rawReviews := strings.Split(rawReviewsBulk, "\n")
	for _, rawReview := range rawReviews {
		json.Unmarshal([]byte(rawReview), &review)

		if (review.UserId != "") {
			mappedReview := comms.UserData {
				UserId:		review.UserId,
			}

			userDataList = append(userDataList, mappedReview)
		} else {
			log.Warnf("Empty UserID detected in raw review %s.", rawReview)
		}
	}

	return userDataList
}

func (mapper *Mapper) sendMappedData(dataset int, bulk int, mappedData []comms.UserData) {
	dataListByPartition := make(map[string][]comms.UserData)

	for _, data := range mappedData {
		partition := mapper.outputPartitions[string(data.UserId[0])]

		if partition == "" {
			partition = proc.DefaultPartition
			log.Errorf("Couldn't calculate partition for user '%s'. Setting default (%s).", data.UserId, partition)
		}

		userDataListPartitioned := dataListByPartition[partition]
		if userDataListPartitioned != nil {
			dataListByPartition[partition] = append(userDataListPartitioned, data)
		} else {
			dataListByPartition[partition] = append(make([]comms.UserData, 0), data)
		}
	}

	for partition, userDataListPartitioned := range dataListByPartition {
		bytes, err := json.Marshal(userDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%v). Err: '%s'", userDataListPartitioned, err)
		} else {
			outputData := comms.SignMessage(props.MapperM5_Name, dataset, mapper.instance, bulk, string(bytes))
			err := mapper.outputDirect.PublishData([]byte(outputData), partition)

			if err != nil {
				log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulk, mapper.outputDirect.Exchange, partition, err)
			} else {
				logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to direct-exchange %s (partition %s).", bulk, mapper.outputDirect.Exchange, partition), bulk)
			}	
		}
	}
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing User Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
