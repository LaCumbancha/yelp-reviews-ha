package core

import (
	"fmt"
	"strings"
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	utils "github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
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
	UserAggregators		int
}

type Mapper struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	inputDirect 		*rabbit.RabbitInputDirect
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals 			int
}

func NewMapper(config MapperConfig) *Mapper {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.ReviewsScatterOutput, props.UserMapperTopic, props.UserMapperInput)
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.UserMapperOutput)

	mapper := &Mapper {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		inputDirect:		inputDirect,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.UserAggregators, PartitionableValues),
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
	rabbit.OutputDirectFinish(comms.FinishMessageSigned(datasetNumber, mapper.instance), mapper.outputPartitions, mapper.outputDirect)
}

func (mapper *Mapper) closeCallback() {
	// TODO
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

func (mapper *Mapper) sendMappedData(datasetNumber int, bulkNumber int, mappedBulk []comms.UserData) {
	dataListByPartition := make(map[string][]comms.UserData)

	for _, data := range mappedBulk {
		partition := mapper.outputPartitions[string(data.UserId[0])]

		if partition != "" {
			userDataListPartitioned := dataListByPartition[partition]

			if userDataListPartitioned != nil {
				dataListByPartition[partition] = append(userDataListPartitioned, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.UserData, 0), data)
			}

		} else {
			log.Errorf("Couldn't calculate partition for weekday (%s).", data.UserId)
		}
	}

	for partition, userDataListPartitioned := range dataListByPartition {
		bytes, err := json.Marshal(userDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", userDataListPartitioned, err)
		} else {
			outputData := comms.SignMessage(datasetNumber, mapper.instance, bulkNumber, string(bytes))
			err := mapper.outputDirect.PublishData([]byte(outputData), partition)

			if err != nil {
				log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulkNumber, mapper.outputDirect.Exchange, partition, err)
			} else {
				logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to direct-exchange %s (partition %s).", bulkNumber, mapper.outputDirect.Exchange, partition), bulkNumber)
			}	
		}
	}
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing User Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
