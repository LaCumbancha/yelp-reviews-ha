package core

import (
	"fmt"
	"strings"
	"crypto/md5"
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

type MapperConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	ReviewsInputs		int
	BotsAggregators		int
}

type Mapper struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	inputFanout 		*rabbit.RabbitInputFanout
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignalsNeeded	map[string]int
}

func NewMapper(config MapperConfig) *Mapper {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputFanout := rabbit.NewRabbitInputFanout(channel, props.InputI2_Output, props.MapperM4_Input)
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.MapperM4_Output)

	endSignalsNeeded := map[string]int{props.InputI2_Name: config.ReviewsInputs}

	mapper := &Mapper {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		inputFanout:		inputFanout,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.BotsAggregators, PartitionableValues),
		endSignalsNeeded:	endSignalsNeeded,
	}

	return mapper
}

func (mapper *Mapper) Run() {
	log.Infof("Starting to listen for reviews.")
	dataByInput := map[string]<-chan amqp.Delivery{props.InputI2_Name: mapper.inputFanout.ConsumeData()}
	mainCallbackByInput := map[string]func(string, int, string, int, string){props.InputI2_Name: mapper.mainCallback}
	
	proc.ProcessInputsStatelessly(
		dataByInput,
		mapper.workersPool,
		mapper.endSignalsNeeded,
		[]string{},
		mainCallbackByInput,
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
	rabbit.OutputDirectStart(comms.StartMessageSigned(props.MapperM4_Name, dataset, mapper.instance), mapper.outputPartitions, mapper.outputDirect)
}

func (mapper *Mapper) finishCallback(dataset int) {
	// Sending Finish-Message to consumers.
	rabbit.OutputDirectFinish(comms.FinishMessageSigned(props.MapperM4_Name, dataset, mapper.instance), mapper.outputPartitions, mapper.outputDirect)
}

func (mapper *Mapper) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputDirectClose(comms.CloseMessageSigned(props.MapperM4_Name, mapper.instance), mapper.outputPartitions, mapper.outputDirect)
}

func (mapper *Mapper) mapData(rawReviewsBulk string) []comms.HashedTextData {
	var review comms.FullReview
	var hashTextDataList []comms.HashedTextData

	rawReviews := strings.Split(rawReviewsBulk, "\n")
	for _, rawReview := range rawReviews {
		json.Unmarshal([]byte(rawReview), &review)

		if (review.UserId != "") {
			hasher := md5.New()
			hasher.Write([]byte(review.Text))
			hashedText := fmt.Sprintf("%x", hasher.Sum(nil))
	
			mappedReview := comms.HashedTextData {
				UserId:			review.UserId,
				HashedText:		hashedText,
			}

			hashTextDataList = append(hashTextDataList, mappedReview)			
		} else {
			log.Warnf("Empty UserID detected in raw review %s.", rawReview)
		}
	}

	return hashTextDataList
}

func (mapper *Mapper) sendMappedData(dataset int, bulk int, mappedData []comms.HashedTextData) {
	dataListByPartition := make(map[string][]comms.HashedTextData)

	for _, data := range mappedData {
		partition := mapper.outputPartitions[string(data.UserId[0])]

		if partition == "" {
			partition = proc.DefaultPartition
			log.Errorf("Couldn't calculate partition for user '%s'. Setting default (%s).", data.UserId, partition)
		}

		hashedDataList := dataListByPartition[partition]
		if hashedDataList != nil {
			dataListByPartition[partition] = append(hashedDataList, data)
		} else {
			dataListByPartition[partition] = append(make([]comms.HashedTextData, 0), data)
		}
	}

	for partition, userDataListPartitioned := range dataListByPartition {
		bytes, err := json.Marshal(userDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", userDataListPartitioned, err)
		} else {
			outputData := comms.SignMessage(props.MapperM4_Name, dataset, mapper.instance, bulk, string(bytes))
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
	log.Infof("Closing User Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
