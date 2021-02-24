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

type AggregatorConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	InputTopic			string
	FunbizFilters 		int
	FuncitJoiners		int
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
	endSignalsNeeded	map[string]int
}

func NewAggregator(config AggregatorConfig) *Aggregator {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.FilterF1_Output, config.InputTopic, rabbit.InnerQueueName(props.AggregatorA1_Input, config.Instance))
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.AggregatorA1_Output)

	endSignalsNeeded := map[string]int{props.FilterF1_Name: config.FunbizFilters}

	aggregator := &Aggregator {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(config.OutputBulkSize),
		inputDirect:		inputDirect,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.FuncitJoiners, PartitionableValues),
		endSignalsNeeded:	endSignalsNeeded,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for funny-business data.")
	dataByInput := map[string]<-chan amqp.Delivery{props.FilterF1_Name: aggregator.inputDirect.ConsumeData()}
	mainCallbackByInput := map[string]func(string, int, string, int, string){props.FilterF1_Name: aggregator.mainCallback}
	backupCallbackByInput := map[string]func(int, []string){props.FilterF1_Name: aggregator.calculator.LoadBackup}

	proc.ProcessInputsStatefully(
		dataByInput,
		aggregator.workersPool,
		aggregator.endSignalsNeeded,
		[]string{},
		mainCallbackByInput,
		backupCallbackByInput,
		aggregator.startCallback,
		aggregator.finishCallback,
		aggregator.closeCallback,
	)
}

func (aggregator *Aggregator) mainCallback(inputNode string, dataset int, instance string, bulk int, data string) {
	aggregator.calculator.Save(dataset, bulk, data)
}

func (aggregator *Aggregator) startCallback(dataset int) {
	// Initializing new dataset in Calculator.
	aggregator.calculator.RegisterDataset(dataset)

	// Sending Start-Message to consumers.
	rabbit.OutputDirectStart(comms.StartMessageSigned(props.AggregatorA1_Name, dataset, aggregator.instance), aggregator.outputPartitions, aggregator.outputDirect)
}

func (aggregator *Aggregator) finishCallback(dataset int) {
	// Computing aggregations.
	outputBulk := 0
	for _, aggregatedData := range aggregator.calculator.AggregateData(dataset) {
		outputBulk++
		logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d generated.", outputBulk), outputBulk)
		aggregator.sendAggregatedData(dataset, outputBulk, aggregatedData)
	}

	// Removing processed dataset from Calculator.
	aggregator.calculator.Clear(dataset)

	// Sending Finish-Message to consumers.
	rabbit.OutputDirectFinish(comms.FinishMessageSigned(props.AggregatorA1_Name, dataset, aggregator.instance), aggregator.outputPartitions, aggregator.outputDirect)
}

func (aggregator *Aggregator) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputDirectClose(comms.CloseMessageSigned(props.AggregatorA1_Name, aggregator.instance), aggregator.outputPartitions, aggregator.outputDirect)
}

func (aggregator *Aggregator) sendAggregatedData(dataset int, bulk int, aggregatedData []comms.FunnyBusinessData) {
	dataListByPartition := make(map[string][]comms.FunnyBusinessData)

	for _, data := range aggregatedData {
		partition := aggregator.outputPartitions[string(data.BusinessId[0])]

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

	for partition, funbizDataListPartitioned := range dataListByPartition {
		bytes, err := json.Marshal(funbizDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%v). Err: '%s'", funbizDataListPartitioned, err)
		} else {
			data := comms.SignMessage(props.AggregatorA1_Name, dataset, aggregator.instance, bulk, string(bytes))
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
	log.Infof("Closing Funny-Business Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}
