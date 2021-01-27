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
	BusinessesInputs	int
	FuncitJoiners 		int
}

type Mapper struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	inputQueue 			*rabbit.RabbitInputQueue
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals			int
}

func NewMapper(config MapperConfig) *Mapper {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.BusinessesScatterOutput)
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.CitbizMapperOutput)

	mapper := &Mapper {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		inputQueue:			inputQueue,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.FuncitJoiners, PartitionableValues),
		endSignals:			config.BusinessesInputs,
	}

	return mapper
}

func (mapper *Mapper) Run() {
	log.Infof("Starting to listen for business.")
	proc.Transformation(
		mapper.workersPool,
		mapper.endSignals,
		mapper.inputQueue.ConsumeData(),
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

func (mapper *Mapper) mapData(rawBusinessesBulk string) []comms.CityBusinessData {
	var business comms.FullBusiness
	var citbizDataList []comms.CityBusinessData

	rawBusinesses := strings.Split(rawBusinessesBulk, "\n")
	for _, rawBusiness := range rawBusinesses {
		if rawBusiness != "" {
			json.Unmarshal([]byte(rawBusiness), &business)
			
			mappedBusiness := comms.CityBusinessData {
				BusinessId:		business.BusinessId,
				City:			fmt.Sprintf("%s (%s)", business.City, business.State),
			}

			citbizDataList = append(citbizDataList, mappedBusiness)
		} else {
			log.Warnf("Empty RawBusiness.")
		}
	}

	return citbizDataList
}

func (mapper *Mapper) sendMappedData(datasetNumber int, bulkNumber int, mappedBulk []comms.CityBusinessData) {
	dataListByPartition := make(map[string][]comms.CityBusinessData)

	for _, data := range mappedBulk {
		partition := mapper.outputPartitions[string(data.BusinessId[0])]

		if partition != "" {
			citbizDataListPartitioned := dataListByPartition[partition]

			if citbizDataListPartitioned != nil {
				dataListByPartition[partition] = append(citbizDataListPartitioned, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.CityBusinessData, 0), data)
			}

		} else {
			log.Errorf("Couldn't calculate partition for business '%s'.", data.BusinessId)
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
	log.Infof("Closing City-Business Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
