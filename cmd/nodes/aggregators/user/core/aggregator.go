package core

import (
	"fmt"
	"encoding/json"
	"github.com/streadway/amqp"
	
	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/yelp-review-ha/cmd/common/logger"
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
	UserMappers 		int
	UserFilters 		int
	OutputBulkSize		int
}

type Aggregator struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	calculator			*Calculator
	inputDirect 		*rabbit.RabbitInputDirect
	outputQueue		*rabbit.RabbitOutputQueue
	endSignalsNeeded	map[string]int
}

func NewAggregator(config AggregatorConfig) *Aggregator {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.MapperM5_Output, config.InputTopic, rabbit.InnerQueueName(props.AggregatorA7_Input, config.Instance))
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.AggregatorA7_Output, config.UserFilters)

	endSignalsNeeded := map[string]int{props.MapperM5_Name: config.UserMappers}

	aggregator := &Aggregator {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(config.OutputBulkSize),
		inputDirect:		inputDirect,
		outputQueue:		outputQueue,
		endSignalsNeeded:	endSignalsNeeded,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for user reviews data.")
	dataByInput := map[string]<-chan amqp.Delivery{props.MapperM5_Name: aggregator.inputDirect.ConsumeData()}
	mainCallbackByInput := map[string]func(string, int, string, int, string){props.MapperM5_Name: aggregator.mainCallback}
	backupCallbackByInput := map[string]func(int, []string){props.MapperM5_Name: aggregator.calculator.LoadBackup}
	
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
	rabbit.OutputQueueStart(comms.StartMessageSigned(props.AggregatorA7_Name, dataset, aggregator.instance), aggregator.outputQueue)
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
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(props.AggregatorA7_Name, dataset, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputQueueClose(comms.CloseMessageSigned(props.AggregatorA7_Name, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) sendAggregatedData(dataset int, bulk int, aggregatedData []comms.UserData) {
	bytes, err := json.Marshal(aggregatedData)
	if err != nil {
		log.Errorf("Error generating Json from aggregated bulk #%d. Err: '%s'", bulk, err)
	} else {
		data := comms.SignMessage(props.AggregatorA7_Name, dataset, aggregator.instance, bulk, string(bytes))

		err = aggregator.outputQueue.PublishData([]byte(data))
		if err != nil {
			log.Errorf("Error sending aggregated bulk #%d to output queue %s. Err: '%s'", bulk, aggregator.outputQueue.Name, err)
		} else {
			logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d sent to output queue %s.", bulk, aggregator.outputQueue.Name), bulk)
		}
	}
}

func (aggregator *Aggregator) Stop() {
	log.Infof("Closing User Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}
