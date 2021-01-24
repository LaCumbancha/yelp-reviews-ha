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

func ProcessInputs(inputs <- chan amqp.Delivery, storingChannel chan amqp.Delivery, endSignals int, procWg *sync.WaitGroup, connWg *sync.WaitGroup) {
	distinctEndSignals := make(map[string]int)
	distinctCloseSignals := make(map[string]int)

	for message := range inputs {
		messageBody := string(message.Body)

		if comms.IsCloseMessage(messageBody) {
			newCloseReceived, allCloseReceived := comms.LastEndMessage(messageBody, distinctCloseSignals, endSignals)

			if newCloseReceived {
				log.Infof("Close-Message #%d received.", len(distinctCloseSignals))
			}

			if allCloseReceived {
				log.Infof("All Close-Messages were received.")
				connWg.Done()
			}

		} else if comms.IsEndMessage(messageBody) {
			newFinishReceived, allFinishReceived := comms.LastEndMessage(messageBody, distinctEndSignals, endSignals)

			if newFinishReceived {
				log.Infof("End-Message #%d received.", len(distinctEndSignals))
			}

			if allFinishReceived {
				log.Infof("All End-Messages were received.")
				procWg.Done()
			}

		} else {
			procWg.Add(1)
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

func ProcessFinish(callback func(), procWg *sync.WaitGroup, closingConn bool, connMutex *sync.Mutex) {
	for true {
		procWg.Wait()

		connMutex.Lock()
		if closingConn {
			break
		} else {
			callback()
			procWg.Add(1)
		}
		connMutex.Unlock()
	}
}

func CloseConnection(callback func(), procWg *sync.WaitGroup, connWg *sync.WaitGroup, closingConn bool, connMutex *sync.Mutex) {
	// Waiting to recieve the close connection signal.
	connWg.Wait()
	connMutex.Lock()
	closingConn = true
	connMutex.Unlock()

	// Releasing the starting gorouting wait.
	procWg.Done()

	// Waiting for any possible last processing message.
	procWg.Wait()

	callback()
}