package processing

import (
	"sync"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/reviews-analysis/cmd/common/backup"
)

// Common processing for Mappers, Filters, Aggregators and Prettiers.
func ProcessInputs(
	messagesByInput map[string] <-chan amqp.Delivery,
	workersPool int,
	signalsNeededByInput  map[string]int,
	savedInputs []string,
	mainCallbackByInput map[string]func(string, int, string, int, string),
	startCallback func(int),
	finishCallback func(int),
	closeCallback func(),
) {
	dataChannelByInput := make(map[string]chan amqp.Delivery)
	for input, _ := range messagesByInput {
		dataChannelByInput[input] = make(chan amqp.Delivery)
	}

	procWgsByDataset := make(map[int]*sync.WaitGroup)
	procWgsMutex := &sync.Mutex{}
	receivedMutex := &sync.Mutex{}
	finishWg := &sync.WaitGroup{}
	connWg := &sync.WaitGroup{}
	connWg.Add(1)

	startSignals, finishSignals, closeSignals, receivedMsgs := bkp.LoadSignalsBackup()
	InitializeProcessWaitGroups(startSignals, finishSignals, signalsNeededByInput, procWgsByDataset, procWgsMutex)

	go ReceiveInputMessages(
		messagesByInput,
		dataChannelByInput,
		startSignals,
		finishSignals,
		closeSignals,
		signalsNeededByInput,
		savedInputs,
		startCallback,
		finishCallback,
		closeCallback,
		connWg,
		finishWg,
		procWgsByDataset,
		procWgsMutex,
	)

	for input, mainCallback := range mainCallbackByInput {
		dataChannel, found := dataChannelByInput[input]

		if !found {
			log.Fatalf("Data channel not defined for input %d.", input)
		}

		go ProcessData(workersPool, dataChannel, mainCallback, receivedMsgs, receivedMutex, procWgsByDataset, procWgsMutex)
	}

	// Waiting for close procedure.
	connWg.Wait()
}
