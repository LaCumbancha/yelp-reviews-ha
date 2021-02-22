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
	FuncitTops			int
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
	endSignalsNeeded	map[string]int
}

func NewJoiner(config JoinerConfig) *Joiner {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect1 := rabbit.NewRabbitInputDirect(channel, props.MapperM1_Output, config.InputTopic, rabbit.InnerQueueName(props.JoinerJ1_Input1, config.Instance))
	inputDirect2 := rabbit.NewRabbitInputDirect(channel, props.AggregatorA1_Output, config.InputTopic, rabbit.InnerQueueName(props.JoinerJ1_Input2, config.Instance))
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.JoinerJ1_Output)

	endSignalsNeeded := map[string]int{ 
		props.MapperM1_Name: 			config.CitbizMappers,
		props.AggregatorA1_Name: 		config.FunbizAggregators,
	}

	joiner := &Joiner {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(config.OutputBulkSize),
		inputDirect1:		inputDirect1,
		inputDirect2:		inputDirect2,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.FuncitTops, PartitionableValues),
		endSignalsNeeded:	endSignalsNeeded,
	}

	return joiner
}

func (joiner *Joiner) Run() {
	log.Infof("Starting to listen for funny-business and city-business data.")
	dataByInput := map[string]<-chan amqp.Delivery{
		props.MapperM1_Name: 			joiner.inputDirect1.ConsumeData(),
		props.AggregatorA1_Name: 		joiner.inputDirect2.ConsumeData(),
	}
	mainCallbackByInput := map[string]func(string, int, string, int, string){
		props.MapperM1_Name: 			joiner.mainCallback1, 
		props.AggregatorA1_Name:		joiner.mainCallback2,
	}
	
	proc.ProcessInputs(
		dataByInput,
		joiner.workersPool,
		joiner.endSignalsNeeded,
		[]string{props.MapperM1_Name},
		mainCallbackByInput,
		joiner.startCallback,
		joiner.finishCallback,
		joiner.closeCallback,
	)
}

func (joiner *Joiner) mainCallback1(inputNode string, dataset int, instance string, bulk int, data string) {
	joiner.calculator.AddCityBusiness(dataset, bulk, data)
}

func (joiner *Joiner) mainCallback2(inputNode string, dataset int, instance string, bulk int, data string) {
	joiner.calculator.AddFunnyBusiness(dataset, bulk, data)
}

func (joiner *Joiner) startCallback(dataset int) {
	// Initializing new dataset in Calculator.
	joiner.calculator.RegisterDataset(dataset)

	// Sending Start-Message to consumers.
	rabbit.OutputDirectStart(comms.StartMessageSigned(props.JoinerJ1_Name, dataset, joiner.instance), joiner.outputPartitions, joiner.outputDirect)
}

func (joiner *Joiner) finishCallback(dataset int) {
	// Retrieving join matches.
	joinMatches := joiner.calculator.RetrieveMatches(dataset)

	totalJoinMatches := len(joinMatches)
	if totalJoinMatches == 0 {
		log.Warnf("No join match to send.")
	}

	messageNumber := 0
	for _, joinedData := range joinMatches {
		messageNumber++
		joiner.sendJoinedData(dataset, messageNumber, joinedData)
	}

	// Removing processed dataset from Calculator.
	joiner.calculator.Clear(dataset)

	// Sending Finish-Message to consumers.
	rabbit.OutputDirectFinish(comms.FinishMessageSigned(props.JoinerJ1_Name, dataset, joiner.instance), joiner.outputPartitions, joiner.outputDirect)
}

func (joiner *Joiner) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputDirectClose(comms.CloseMessageSigned(props.JoinerJ1_Name, joiner.instance), joiner.outputPartitions, joiner.outputDirect)
}

func (joiner *Joiner) sendJoinedData(dataset int, bulk int, joinedData []comms.FunnyCityData) {
	dataListByPartition := make(map[string][]comms.FunnyCityData)

	for _, data := range joinedData {
		partition := joiner.outputPartitions[string(data.City[0])]

		if partition == "" {
			partition = proc.DefaultPartition
			log.Errorf("Couldn't calculate partition for city '%s'. Setting default (%s).", data.City, partition)
		}

		funcitDataListPartitioned := dataListByPartition[partition]
		if funcitDataListPartitioned != nil {
			dataListByPartition[partition] = append(funcitDataListPartitioned, data)
		} else {
			dataListByPartition[partition] = append(make([]comms.FunnyCityData, 0), data)
		}
	}

	for partition, userDataListPartitioned := range dataListByPartition {
		bytes, err := json.Marshal(userDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", userDataListPartitioned, err)
		} else {
			data := comms.SignMessage(props.JoinerJ1_Name, dataset, joiner.instance, bulk, string(bytes))
			err := joiner.outputDirect.PublishData([]byte(data), partition)

			if err != nil {
				log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulk, joiner.outputDirect.Exchange, partition, err)
			} else {
				logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to direct-exchange %s (partition %s).", bulk, joiner.outputDirect.Exchange, partition), bulk)
			}	
		}
	}
}

func (joiner *Joiner) Stop() {
	log.Infof("Closing Funny-City Joiner connections.")
	joiner.connection.Close()
	joiner.channel.Close()
}
