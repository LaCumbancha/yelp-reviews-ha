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

func ProcessInputs(inputs <- chan amqp.Delivery, mainChannel chan amqp.Delivery, endingChannel chan int, endSignals int, procWg *sync.WaitGroup, connWg *sync.WaitGroup) {
	distinctCloseSignals := make(map[string]int)
	distinctFinishSignals := make(map[int]map[string]int)

	for message := range inputs {
		messageBody := string(message.Body)
		dataset, instance, _, mainMessage := comms.UnsignMessage(messageBody)

		if comms.IsCloseMessage(mainMessage) {
			newCloseReceived, allCloseReceived := comms.CloseControl(instance, distinctCloseSignals, endSignals)

			if newCloseReceived {
				log.Infof("Close-Message #%d received.", len(distinctCloseSignals))
			}

			if allCloseReceived {
				log.Infof("All Close-Messages were received.")
				connWg.Done()
			}

		} else if comms.IsFinishMessage(mainMessage) {
			newFinishReceived, allFinishReceived := comms.FinishControl(dataset, instance, distinctFinishSignals, endSignals)

			if newFinishReceived {
				log.Infof("Finish-Message #%d received.", len(distinctFinishSignals))
			}

			if allFinishReceived {
				log.Infof("All Finish-Messages were received.")
				endingChannel <- dataset
			}

		} else {
			procWg.Add(1)
			mainChannel <- message
		}
	}
}

func InitializeProcessingWorkers(workersPool int, mainChannel chan amqp.Delivery, callback func(int, int, string), wg *sync.WaitGroup) {
	log.Tracef("Initializing %d workers.", workersPool)
	for worker := 1 ; worker <= workersPool ; worker++ {
		log.Tracef("Initializing worker #%d.", worker)
		
		go func() {
			for message := range mainChannel {
				messageBody := string(message.Body)
				dataset, _, bulk, data := comms.UnsignMessage(messageBody)

				if data != "" {
					logb.Instance().Infof(fmt.Sprintf("Data bulk #%d.%d received.", dataset, bulk), bulk)
					callback(dataset, bulk, data)
					rabbit.AckMessage(message)
    				wg.Done()
				} else {
					log.Warnf("Unexpected message received: '%s'", messageBody)
				}
				
			}
		}()
	}
}

func ProcessSingleFinish(endingChannel chan int, callback func(int), procWg *sync.WaitGroup, closingConn bool, connMutex *sync.Mutex) {
	// Send finish message each time a dataset is completed.
	for datasetFinished := range endingChannel {
		callback(datasetFinished)
		procWg.Done()

		connMutex.Lock()
		if closingConn {
			break
		} else {
			procWg.Add(1)
		}
		connMutex.Unlock()
	}
}

func ProcessMultipleFinish(neededInputs int, savedInputs int, endingChannel chan int, callback func(int), procWg *sync.WaitGroup, closingConn bool, connMutex *sync.Mutex) {
	finishSignals := make(map[int]int)

	// Send finish message each time a dataset is completed.
	for datasetFinished := range endingChannel {
		if received, found := finishSignals[datasetFinished]; found {
			if received + 1 == neededInputs {
				callback(datasetFinished)
				procWg.Done()
				finishSignals[datasetFinished] = savedInputs
			} else {
				finishSignals[datasetFinished] = received + 1
			}

			connMutex.Lock()
			if closingConn {
				break
			} else {
				procWg.Add(1)
			}
			connMutex.Unlock()
		} else {
			finishSignals[datasetFinished] = 1
		}
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