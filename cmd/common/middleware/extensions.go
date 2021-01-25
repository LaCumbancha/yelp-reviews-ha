package middleware

import (
	log "github.com/sirupsen/logrus"
	utils "github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
)

func OutputQueueFinish(message []byte, outputQueue *RabbitOutputQueue) {
	errors := false
	for idx := 1; idx <= outputQueue.EndSignals; idx++ {
		err := outputQueue.PublishData(message)

		if err != nil {
			errors = true
			log.Errorf("Error sending End-Message #%d to queue %s. Err: '%s'", idx, outputQueue.Name, err)
		}
	}

	if !errors {
		log.Infof("End-Message sent to queue %s.", outputQueue.Name)
	}
}

func OutputDirectFinish(message []byte, outputPartitions map[string]string, outputDirect *RabbitOutputDirect) {
	for _, partition := range utils.GetMapDistinctValues(outputPartitions) {
    	err := outputDirect.PublishData(message, partition)

    	if err != nil {
			log.Errorf("Error sending End-Message to direct-exchange %s (partition %s). Err: '%s'", outputDirect.Exchange, partition, err)
		} else {
			log.Infof("End-Message sent to direct-exchange %s (partition %s).", outputDirect.Exchange, partition)
		}
    }
}
