package core

import (
	"strings"
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

type AggregatorConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	InputTopic			string
	WeekdayMappers 		int
}

type Aggregator struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	calculator			*Calculator
	inputDirect 		*rabbit.RabbitInputDirect
	outputQueue 		*rabbit.RabbitOutputQueue
	endSignalsNeeded	map[string]int
}

func NewAggregator(config AggregatorConfig) *Aggregator {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.MapperM3_Output, config.InputTopic, rabbit.InnerQueueName(props.AggregatorA4_Input, config.Instance))
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.AggregatorA4_Output, comms.Prettiers)

	endSignalsNeeded := map[string]int{props.MapperM3_Name: config.WeekdayMappers}

	aggregator := &Aggregator {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(),
		inputDirect:		inputDirect,
		outputQueue:		outputQueue,
		endSignalsNeeded:	endSignalsNeeded,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for weekday data.")
	dataByInput := map[string]<-chan amqp.Delivery{props.MapperM3_Name: aggregator.inputDirect.ConsumeData()}
	mainCallbackByInput := map[string]func(string, int, string, int, string){props.MapperM3_Name: aggregator.mainCallback}
	backupCallbackByInput := map[string]func(int, []string){props.MapperM3_Name: aggregator.calculator.LoadBackup}
	
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
	rabbit.OutputQueueStart(comms.StartMessageSigned(props.AggregatorA4_Name, dataset, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) finishCallback(dataset int) {
	// Computing aggregations.
	weekdayNumber := 1
	for _, aggregatedData := range aggregator.calculator.AggregateData(dataset) {
		aggregator.sendAggregatedData(dataset, weekdayNumber, aggregatedData)
		weekdayNumber++
	}

	// Removing processed dataset from Calculator.
	aggregator.calculator.Clear(dataset)

	// Sending Finish-Message to consumers.
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(props.AggregatorA4_Name, dataset, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputQueueClose(comms.CloseMessageSigned(props.AggregatorA4_Name, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) sendAggregatedData(dataset int, weekdayNumber int, aggregatedWeekday comms.WeekdayData) {
	weekday := strings.ToUpper(aggregatedWeekday.Weekday[0:3])
	bytes, err := json.Marshal(aggregatedWeekday)

	if err != nil {
		log.Errorf("Error generating Json from %s aggregated data. Err: '%s'", weekday, err)
	} else {
		data := comms.SignMessage(props.AggregatorA4_Name, dataset, aggregator.instance, weekdayNumber, string(bytes))
		err := aggregator.outputQueue.PublishData([]byte(data))

		if err != nil {
			log.Errorf("Error sending %s aggregated data to output queue %s. Err: '%s'", weekday, aggregator.outputQueue.Name, err)
		} else {
			log.Infof("%s aggregated data sent to output queue %s.", weekday, aggregator.outputQueue.Name)
		}
	}
}

func (aggregator *Aggregator) Stop() {
	log.Infof("Closing Funny-Business Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}
