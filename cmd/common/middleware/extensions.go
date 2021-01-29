package middleware

import (
	log "github.com/sirupsen/logrus"
	utils "github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
)

func sendOutputQueueMessage(messageType string, message []byte, outputQueue *RabbitOutputQueue) {
	errors := false
	for idx := 1; idx <= outputQueue.EndSignals; idx++ {
		err := outputQueue.PublishData(message)

		if err != nil {
			errors = true
			log.Errorf("Error sending %s #%d to queue %s. Err: '%s'", messageType, idx, outputQueue.Name, err)
		}
	}

	if !errors {
		log.Infof("%s sent to queue %s.", messageType, outputQueue.Name)
	}
}

func sendOutputDirectMessage(messageType string, message []byte, outputPartitions map[string]string, outputDirect *RabbitOutputDirect) {
	for _, partition := range utils.GetMapDistinctValues(outputPartitions) {
    	err := outputDirect.PublishData(message, partition)

    	if err != nil {
			log.Errorf("Error sending %s to direct-exchange %s (partition %s). Err: '%s'", messageType, outputDirect.Exchange, partition, err)
		} else {
			log.Infof("%s sent to direct-exchange %s (partition %s).", messageType, outputDirect.Exchange, partition)
		}
    }
}

func sendOutputFanoutMessage(messageType string, message []byte, outputSignals int, outputFanout *RabbitOutputFanout) {
	errors := false
	for idx := 0 ; idx < outputSignals; idx++ {
		err := outputFanout.PublishData(message)

		if err != nil {
			errors = true
			log.Errorf("Error sending %s to fanout-exchange %s. Err: '%s'", messageType, outputFanout.Exchange, err)
		}
	}

	if !errors {
		log.Infof("%s sent to fanout-exchange %s.", messageType, outputFanout.Exchange)
	}
}

func OutputQueueStart(message []byte, outputQueue *RabbitOutputQueue) {
	sendOutputQueueMessage("Start-Message", message, outputQueue)
}

func OutputQueueFinish(message []byte, outputQueue *RabbitOutputQueue) {
	sendOutputQueueMessage("Finish-Message", message, outputQueue)
}

func OutputQueueClose(message []byte, outputQueue *RabbitOutputQueue) {
	sendOutputQueueMessage("Close-Message", message, outputQueue)
}

func OutputDirectStart(message []byte, outputPartitions map[string]string, outputDirect *RabbitOutputDirect) {
	sendOutputDirectMessage("Start-Message", message, outputPartitions, outputDirect)
}

func OutputDirectFinish(message []byte, outputPartitions map[string]string, outputDirect *RabbitOutputDirect) {
	sendOutputDirectMessage("Finish-Message", message, outputPartitions, outputDirect)
}

func OutputDirectClose(message []byte, outputPartitions map[string]string, outputDirect *RabbitOutputDirect) {
	sendOutputDirectMessage("Close-Message", message, outputPartitions, outputDirect)
}

func OutputFanoutStart(message []byte, outputSignals int, outputFanout *RabbitOutputFanout) {
	sendOutputFanoutMessage("Start-Message", message, outputSignals, outputFanout)
}

func OutputFanoutFinish(message []byte, outputSignals int, outputFanout *RabbitOutputFanout) {
	sendOutputFanoutMessage("Finish-Message", message, outputSignals, outputFanout)
}

func OutputFanoutClose(message []byte, outputSignals int, outputFanout *RabbitOutputFanout) {
	sendOutputFanoutMessage("Close-Message", message, outputSignals, outputFanout)
}
