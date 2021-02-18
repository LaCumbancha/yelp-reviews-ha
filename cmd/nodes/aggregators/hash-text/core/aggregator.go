package core

import (
	"fmt"
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

const NODE_CODE = "A5"

type AggregatorConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	InputTopic			string
	HashMappers 		int
	DishashAggregators 	int
	OutputBulkSize		int
}

type Aggregator struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	calculator			*Calculator
	inputDirect 		*rabbit.RabbitInputDirect
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals			int
}

func NewAggregator(config AggregatorConfig) *Aggregator {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.MapperM4_Output, config.InputTopic, "")
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.AggregatorA5_Output)

	aggregator := &Aggregator {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(config.OutputBulkSize),
		inputDirect:		inputDirect,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.DishashAggregators, PartitionableValues),
		endSignals:			config.HashMappers,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for hashed-text reviews data.")
	proc.Transformation(
		aggregator.workersPool,
		aggregator.endSignals,
		aggregator.inputDirect.ConsumeData(),
		aggregator.mainCallback,
		aggregator.startCallback,
		aggregator.finishCallback,
		aggregator.closeCallback,
	)
}

func (aggregator *Aggregator) mainCallback(inputNode string, dataset int, instance string, bulk int, data string) {
	aggregator.calculator.Save(inputNode, dataset, instance, bulk, data)
}

func (aggregator *Aggregator) startCallback(dataset int) {
	// Clearing Calculator for next dataset.
	aggregator.calculator.Clear(dataset)

	// Sending Start-Message to consumers.
	rabbit.OutputDirectStart(comms.StartMessageSigned(NODE_CODE, dataset, aggregator.instance), aggregator.outputPartitions, aggregator.outputDirect)
}

func (aggregator *Aggregator) finishCallback(dataset int) {
	// Calculating aggregations
	outputBulk := 0
	for _, aggregatedData := range aggregator.calculator.AggregateData(dataset) {
		outputBulk++
		logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d generated.", outputBulk), outputBulk)
		aggregator.sendAggregatedData(dataset, outputBulk, aggregatedData)
	}

	// Sending Finish-Message to consumers.
	rabbit.OutputDirectFinish(comms.FinishMessageSigned(NODE_CODE, dataset, aggregator.instance), aggregator.outputPartitions, aggregator.outputDirect)
}

func (aggregator *Aggregator) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputDirectClose(comms.CloseMessageSigned(NODE_CODE, aggregator.instance), aggregator.outputPartitions, aggregator.outputDirect)
}

func (aggregator *Aggregator) sendAggregatedData(dataset int, bulk int, aggregatedData []comms.HashedTextData) {
	dataListByPartition := make(map[string][]comms.HashedTextData)

	for _, data := range aggregatedData {
		partition := aggregator.outputPartitions[string(data.UserId[0])]

		if partition == "" {
			partition = proc.DefaultPartition
			log.Errorf("Couldn't calculate partition for user '%s'. Setting default (%s).", data.UserId, partition)
		}

		hashedTextDataListPartitioned := dataListByPartition[partition]
		if hashedTextDataListPartitioned != nil {
			dataListByPartition[partition] = append(hashedTextDataListPartitioned, data)
		} else {
			dataListByPartition[partition] = append(make([]comms.HashedTextData, 0), data)
		}
	}

	for partition, funbizDataListPartitioned := range dataListByPartition {
		bytes, err := json.Marshal(funbizDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", funbizDataListPartitioned, err)
		} else {
			data := comms.SignMessage(NODE_CODE, dataset, aggregator.instance, bulk, string(bytes))
			err := aggregator.outputDirect.PublishData([]byte(data), partition)

			if err != nil {
				log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulk, aggregator.outputDirect.Exchange, partition, err)
			} else {
				logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d sent to direct-exchange %s (partition %s).", bulk, aggregator.outputDirect.Exchange, partition), bulk)
			}	
		}
	}
}

func (aggregator *Aggregator) Stop() {
	log.Infof("Closing Hash-Text Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}
