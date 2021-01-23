package processing

import (
	"fmt"
	"sync"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

func ProcessInputs(inputs <- chan amqp.Delivery, storingChannel chan amqp.Delivery, endSignals int, wg *sync.WaitGroup) {
	distinctEndSignals := make(map[string]int)

	for message := range inputs {
		messageBody := string(message.Body)

		if comms.IsEndMessage(messageBody) {
			newFinishReceived, allFinishReceived := comms.LastEndMessage(messageBody, distinctEndSignals, endSignals)

			if newFinishReceived {
				log.Infof("End-Message #%d received.", len(distinctEndSignals))
			}

			if allFinishReceived {
				log.Infof("All End-Messages were received.")
				wg.Done()
			}

		} else {
			wg.Add(1)
			storingChannel <- message
		}
	}
}

func InitializeProcessingWorkers(workersPool int, storingChannel chan amqp.Delivery, callback func(int, string), wg *sync.WaitGroup) {
	bulkNumber := 0
	bulkNumberMutex := &sync.Mutex{}

	log.Infof("Initializing %d workers.", workersPool)
	for worker := 1 ; worker <= workersPool ; worker++ {
		log.Infof("Initializing worker #%d.", worker)
		
		go func() {
			for message := range storingChannel {
				bulkNumberMutex.Lock()
				bulkNumber++
				innerBulk := bulkNumber
				bulkNumberMutex.Unlock()

				logb.Instance().Infof(fmt.Sprintf("Data bulk #%d received.", innerBulk), innerBulk)

				callback(bulkNumber, string(message.Body))
				rabbit.AckMessage(message)
    			wg.Done()
			}
		}()
	}
}
