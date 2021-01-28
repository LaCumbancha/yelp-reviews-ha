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

const DefaultFlow = ""
const DefaultPartition = "0"

func ProcessInputs(flow string, inputs <- chan amqp.Delivery, mainChannel chan amqp.Delivery, endingChannel chan int, endSignals int, procWg *sync.WaitGroup, connWg *sync.WaitGroup) {
	distinctCloseSignals := make(map[string]int)
	distinctFinishSignals := make(map[int]map[string]int)

	for message := range inputs {
		messageBody := string(message.Body)
		_, dataset, instance, _, mainMessage := comms.UnsignMessage(messageBody)

		if comms.IsCloseMessage(mainMessage) {
			newCloseReceived, allCloseReceived := comms.CloseControl(instance, distinctCloseSignals, endSignals)

			if newCloseReceived {
				flowMessageLog(fmt.Sprintf("Close-Message #%d", len(distinctCloseSignals)), flow)
			}

			if allCloseReceived {
				flowMessageLog("All Close-Message", flow)
				connWg.Done()
			}

		} else if comms.IsFinishMessage(mainMessage) {
			newFinishReceived, allFinishReceived := comms.FinishControl(dataset, instance, distinctFinishSignals, endSignals)

			if newFinishReceived {
				flowMessageLog(fmt.Sprintf("Finish-Message #%d", len(distinctFinishSignals[dataset])), flow)
			}

			if allFinishReceived {
				procWg.Done()
				flowMessageLog("All Finish-Message", flow)
				endingChannel <- dataset
			}

		} else {
			procWg.Add(1)
			mainChannel <- message
		}
	}
}

func flowMessageLog(messageType string, flow string) {
	if flow == DefaultFlow {
		log.Infof("%s received.", messageType)
	} else {
		log.Infof("%s from the %s flow received.", messageType, flow)
	}
}

func InitializeProcessingWorkers(workersPool int, mainChannel chan amqp.Delivery, callback func(string, int, string, int, string), wg *sync.WaitGroup) {
	log.Tracef("Initializing %d workers.", workersPool)
	for worker := 1 ; worker <= workersPool ; worker++ {
		log.Tracef("Initializing worker #%d.", worker)
		
		go func() {
			for message := range mainChannel {
				messageBody := string(message.Body)
				inputNode, dataset, instance, bulk, data := comms.UnsignMessage(messageBody)

				if data != "" {
					logb.Instance().Infof(fmt.Sprintf("Message #%s.%d.%s.%d received.", inputNode, dataset, instance, bulk), bulk)
					callback(inputNode, dataset, instance, bulk, data)
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
		procWg.Wait()
		callback(datasetFinished)

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
			finishSignals[datasetFinished] = received + 1
		} else {
			finishSignals[datasetFinished] = savedInputs + 1
		}

		if finishSignals[datasetFinished] == neededInputs {
			procWg.Wait()
			callback(datasetFinished)

			connMutex.Lock()
			if closingConn {
				break
			} else {
				procWg.Add(neededInputs - savedInputs)
			}
			connMutex.Unlock()
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