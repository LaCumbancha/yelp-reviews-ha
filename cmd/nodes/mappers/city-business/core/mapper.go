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

const NODE_CODE = "M1"

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
	rabbit.OutputDirectStart(comms.StartMessageSigned(NODE_CODE, dataset, mapper.instance), mapper.outputPartitions, mapper.outputDirect)
}

func (mapper *Mapper) finishCallback(dataset int) {
	// Sending Finish-Message to consumers.
	rabbit.OutputDirectFinish(comms.FinishMessageSigned(NODE_CODE, dataset, mapper.instance), mapper.outputPartitions, mapper.outputDirect)
}

func (mapper *Mapper) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputDirectClose(comms.CloseMessageSigned(NODE_CODE, mapper.instance), mapper.outputPartitions, mapper.outputDirect)
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

func (mapper *Mapper) sendMappedData(dataset int, bulk int, mappedData []comms.CityBusinessData) {
	dataListByPartition := make(map[string][]comms.CityBusinessData)

	for _, data := range mappedData {
		partition := mapper.outputPartitions[string(data.BusinessId[0])]

		if partition == "" {
			partition = proc.DefaultPartition
			log.Errorf("Couldn't calculate partition for business '%s'. Setting default (%s).", data.BusinessId, partition)
		}

		citbizDataListPartitioned := dataListByPartition[partition]
		if citbizDataListPartitioned != nil {
			dataListByPartition[partition] = append(citbizDataListPartitioned, data)
		} else {
			dataListByPartition[partition] = append(make([]comms.CityBusinessData, 0), data)
		}
	}

	for partition, userDataListPartitioned := range dataListByPartition {
		bytes, err := json.Marshal(userDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", userDataListPartitioned, err)
		} else {
			outputData := comms.SignMessage(NODE_CODE, dataset, mapper.instance, bulk, string(bytes))
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
	log.Infof("Closing City-Business Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
