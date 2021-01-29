package core

import (
	"fmt"
	"time"
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

const NODE_CODE = "M3"

type MapperConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	WeekdayAggregators 	int
	ReviewsInputs 		int
}

type Mapper struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	inputFanout 		*rabbit.RabbitInputFanout
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals			int
}

func NewMapper(config MapperConfig) *Mapper {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputFanout := rabbit.NewRabbitInputFanout(channel, props.ReviewsScatterOutput, props.WeekdayMapperInput)
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.WeekdayMapperOutput)

	mapper := &Mapper {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		inputFanout:		inputFanout,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.WeekdayAggregators, PartitionableValues),
		endSignals:			config.ReviewsInputs,
	}

	return mapper
}

func (mapper *Mapper) Run() {
	log.Infof("Starting to listen for reviews.")
	proc.Transformation(
		mapper.workersPool,
		mapper.endSignals,
		mapper.inputFanout.ConsumeData(),
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

func (mapper *Mapper) mapData(rawReviewsBulk string) []comms.WeekdayData {
	var review comms.FullReview
	var weekdayDataList []comms.WeekdayData

	rawReviews := strings.Split(rawReviewsBulk, "\n")
	for _, rawReview := range rawReviews {
		json.Unmarshal([]byte(rawReview), &review)

		if rawReview != "" {
			reviewDate, err := time.Parse("2006-01-02", review.Date[0:10])

			if err != nil {
				log.Errorf("Error parsing date from review %s (given date: %s). Err: %s", review.ReviewId, review.Date, err)
			} else {
				reviewWeekday := reviewDate.Weekday()
				mappedReview := comms.WeekdayData {
					Weekday:	reviewWeekday.String(),
				}

				weekdayDataList = append(weekdayDataList, mappedReview)
			}	
		} else {
			log.Warnf("Empty RawReview.")
		}	
	}

	return weekdayDataList
}

func (mapper *Mapper) sendMappedData(dataset int, bulk int, mappedData []comms.WeekdayData) {
	dataListByPartition := make(map[string][]comms.WeekdayData)

	for _, data := range mappedData {
		partition := mapper.outputPartitions[data.Weekday]

		if partition == "" {
			partition = proc.DefaultPartition
			log.Errorf("Couldn't calculate partition for weekday '%s'. Setting default (%s).", data.Weekday, partition)
		}

		weekdayDataListPartitioned := dataListByPartition[partition]
		if weekdayDataListPartitioned != nil {
			dataListByPartition[partition] = append(weekdayDataListPartitioned, data)
		} else {
			dataListByPartition[partition] = append(make([]comms.WeekdayData, 0), data)
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
	log.Infof("Closing Weekday Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
