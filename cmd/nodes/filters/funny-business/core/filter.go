package core

import (
	"fmt"
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

type FilterConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	FunbizMappers 		int
	FunbizAggregators	int
}

type Filter struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	inputQueue 			*rabbit.RabbitInputQueue
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignalsNeeded	map[string]int
}

func NewFilter(config FilterConfig) *Filter {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.MapperM2_Output)
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.FilterF1_Output)

	endSignalsNeeded := map[string]int{props.MapperM2_Name: config.FunbizMappers}

	filter := &Filter {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		inputQueue:			inputQueue,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.FunbizAggregators, PartitionableValues),
		endSignalsNeeded:	endSignalsNeeded,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for funny-business data.")
	dataByInput := map[string]<-chan amqp.Delivery{props.MapperM2_Name: filter.inputQueue.ConsumeData()}
	mainCallbackByInput := map[string]func(string, int, string, int, string){props.MapperM2_Name: filter.mainCallback}
	
	proc.ProcessInputsStatelessly(
		dataByInput,
		filter.workersPool,
		filter.endSignalsNeeded,
		[]string{},
		mainCallbackByInput,
		filter.startCallback,
		filter.finishCallback,
		filter.closeCallback,
	)
}

func (filter *Filter) mainCallback(inputNode string, dataset int, instance string, bulk int, data string) {
	filteredData := filter.filterData(data)
	filter.sendFilteredData(dataset, bulk, filteredData)
}

func (filter *Filter) startCallback(dataset int) {
	// Sending Start-Message to consumers.
	rabbit.OutputDirectStart(comms.StartMessageSigned(props.FilterF1_Name, dataset, filter.instance), filter.outputPartitions, filter.outputDirect)
}

func (filter *Filter) finishCallback(dataset int) {
	// Sending Finish-Message to consumers.
	rabbit.OutputDirectFinish(comms.FinishMessageSigned(props.FilterF1_Name, dataset, filter.instance), filter.outputPartitions, filter.outputDirect)
}

func (filter *Filter) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputDirectClose(comms.CloseMessageSigned(props.FilterF1_Name, filter.instance), filter.outputPartitions, filter.outputDirect)
}

func (filter *Filter) filterData(rawFunbizDataBulk string) []comms.FunnyBusinessData {
	var funbizDataList []comms.FunnyBusinessData
	var filteredFunbizDataList []comms.FunnyBusinessData
	json.Unmarshal([]byte(rawFunbizDataBulk), &funbizDataList)

	for _, funbizData := range funbizDataList {
		if (funbizData.Funny > 0) {
			filteredFunbizDataList = append(filteredFunbizDataList, funbizData)	
		}
	}

	return filteredFunbizDataList
}

func (filter *Filter) sendFilteredData(dataset int, bulk int, filteredData []comms.FunnyBusinessData) {
	dataListByPartition := make(map[string][]comms.FunnyBusinessData)

	for _, data := range filteredData {
		partition := filter.outputPartitions[string(data.BusinessId[0])]

		if partition == "" {
			partition = proc.DefaultPartition
			log.Errorf("Couldn't calculate partition for business '%s'. Setting default (%s).", data.BusinessId, partition)
		}

		funbizDataListPartitioned := dataListByPartition[partition]
		if funbizDataListPartitioned != nil {
			dataListByPartition[partition] = append(funbizDataListPartitioned, data)
		} else {
			dataListByPartition[partition] = append(make([]comms.FunnyBusinessData, 0), data)
		}
	}

	for partition, userDataListPartitioned := range dataListByPartition {
		bytes, err := json.Marshal(userDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%v). Err: '%s'", userDataListPartitioned, err)
		} else {
			data := comms.SignMessage(props.FilterF1_Name, dataset, filter.instance, bulk, string(bytes))
			err := filter.outputDirect.PublishData([]byte(data), partition)

			if err != nil {
				log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulk, filter.outputDirect.Exchange, partition, err)
			} else {
				logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to direct-exchange %s (partition %s).", bulk, filter.outputDirect.Exchange, partition), bulk)
			}	
		}
	}
}

func (filter *Filter) Stop() {
	log.Infof("Closing Funny-Business Filter connections.")
	filter.connection.Close()
	filter.channel.Close()
}
