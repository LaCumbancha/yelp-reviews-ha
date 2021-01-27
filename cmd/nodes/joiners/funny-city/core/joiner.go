package core

import (
	"fmt"
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	utils "github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

type JoinerConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	InputTopic			string
	FunbizAggregators 	int
	CitbizMappers		int
	FuncitAggregators	int
	OutputBulkSize		int
}

type Joiner struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	calculator			*Calculator
	inputDirect1 		*rabbit.RabbitInputDirect
	inputDirect2 		*rabbit.RabbitInputDirect
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals1			int
	endSignals2			int
}

func NewJoiner(config JoinerConfig) *Joiner {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect1 := rabbit.NewRabbitInputDirect(channel, props.FunbizAggregatorOutput, config.InputTopic, "")
	inputDirect2 := rabbit.NewRabbitInputDirect(channel, props.CitbizMapperOutput, config.InputTopic, "")
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.FuncitJoinerOutput)

	joiner := &Joiner {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(config.OutputBulkSize),
		inputDirect1:		inputDirect1,
		inputDirect2:		inputDirect2,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.FuncitAggregators, PartitionableValues),
		endSignals1:		config.FunbizAggregators,
		endSignals2:		config.CitbizMappers,
	}

	return joiner
}

func (joiner *Joiner) Run() {
	neededInputs := 2
	savedInputs := 1
	log.Infof("Starting to listen for funny-business and city-business data.")
	proc.Join(
		neededInputs,
		savedInputs,
		joiner.workersPool,
		joiner.endSignals1,
		joiner.endSignals2,
		joiner.inputDirect1.ConsumeData(),
		joiner.inputDirect2.ConsumeData(),
		joiner.mainCallback1,
		joiner.mainCallback2,
		joiner.finishCallback,
		joiner.closeCallback,
	)
}

func (joiner *Joiner) mainCallback1(datasetNumber int, bulkNumber int, bulk string) {
	joiner.calculator.AddFunnyBusiness(datasetNumber, bulkNumber, bulk)
}

func (joiner *Joiner) mainCallback2(datasetNumber int, bulkNumber int, bulk string) {
	joiner.calculator.AddCityBusiness(datasetNumber, bulkNumber, bulk)
}

func (joiner *Joiner) finishCallback(datasetNumber int) {
	// Retrieving join matches.
	joinMatches := joiner.calculator.RetrieveMatches(datasetNumber)

	if len(joinMatches) == 0 {
		log.Warnf("No join match to send.")
	}

	messageCounter := 0
	for _, joinedData := range joinMatches {
		messageCounter++
		joiner.sendJoinedData(datasetNumber, messageCounter, joinedData)
	}

	// Clearing Calculator for next dataset.
	joiner.calculator.Clear()

	// Sending End-Message to consumers.
	rabbit.OutputDirectFinish(comms.FinishMessageSigned(datasetNumber, joiner.instance), joiner.outputPartitions, joiner.outputDirect)
}

func (joiner *Joiner) closeCallback() {
	// TODO
}

func (joiner *Joiner) sendJoinedData(datasetNumber int, bulkNumber int, joinedBulk []comms.FunnyCityData) {
	dataListByPartition := make(map[string][]comms.FunnyCityData)

	for _, data := range joinedBulk {
		partition := joiner.outputPartitions[string(data.City[0])]

		if partition != "" {
			funcitDataListPartitioned := dataListByPartition[partition]

			if funcitDataListPartitioned != nil {
				dataListByPartition[partition] = append(funcitDataListPartitioned, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.FunnyCityData, 0), data)
			}

		} else {
			log.Errorf("Couldn't calculate partition for city '%s'.", data.City)
		}
	}

	for partition, userDataListPartitioned := range dataListByPartition {
		bytes, err := json.Marshal(userDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", userDataListPartitioned, err)
		} else {
			data := comms.SignMessage(datasetNumber, joiner.instance, bulkNumber, string(bytes))
			err := joiner.outputDirect.PublishData([]byte(data), partition)

			if err != nil {
				log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulkNumber, joiner.outputDirect.Exchange, partition, err)
			} else {
				logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to direct-exchange %s (partition %s).", bulkNumber, joiner.outputDirect.Exchange, partition), bulkNumber)
			}	
		}
	}
}

func (joiner *Joiner) Stop() {
	log.Infof("Closing Funny-City Joiner connections.")
	joiner.connection.Close()
	joiner.channel.Close()
}
