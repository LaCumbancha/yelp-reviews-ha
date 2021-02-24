package processing

import (
	"sync"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/reviews-analysis/cmd/common/backup"
)

func ProcessInputsStatefully(
	messagesByInput map[string] <-chan amqp.Delivery,
	workersPool int,
	signalsNeededByInput  map[string]int,
	savedInputs []string,
	mainCallbackByInput map[string]func(string, int, string, int, string),
	backupCallbackByInput map[string]func(int, []string),
	startCallback func(int),
	finishCallback func(int),
	closeCallback func(),
) {
	processInputs(
		messagesByInput, 
		workersPool,
		bkp.FullBackup,
		signalsNeededByInput,
		savedInputs,
		mainCallbackByInput,
		backupCallbackByInput,
		startCallback,
		finishCallback,
		closeCallback,
	)
}

func ProcessInputsStatelessly(
	messagesByInput map[string] <-chan amqp.Delivery,
	workersPool int,
	signalsNeededByInput  map[string]int,
	savedInputs []string,
	mainCallbackByInput map[string]func(string, int, string, int, string),
	startCallback func(int),
	finishCallback func(int),
	closeCallback func(),
) {
	processInputs(
		messagesByInput,
		workersPool,
		bkp.IdBackup,
		signalsNeededByInput,
		savedInputs, 
		mainCallbackByInput, 
		make(map[string]func(int, []string)), 
		startCallback, 
		finishCallback, 
		closeCallback,
	)
}

// Common processing for Mappers, Filters, Aggregators and Prettiers.
func processInputs(
	messagesByInput map[string] <-chan amqp.Delivery,
	workersPool int,
	receivedBkpMode bkp.BackupMode,
	signalsNeededByInput  map[string]int,
	savedInputs []string,
	mainCallbackByInput map[string]func(string, int, string, int, string),
	backupCallbackByInput map[string]func(int, []string),
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
	receivedBkpMutex := &sync.Mutex{}
	finishWg := &sync.WaitGroup{}
	connWg := &sync.WaitGroup{}
	connWg.Add(1)

	startSignals, finishSignals, closeSignals := bkp.LoadSignalsBackup()
	initializeProcessWaitGroups(startSignals, finishSignals, signalsNeededByInput, procWgsByDataset, procWgsMutex)

	receivedMsgs := bkp.LoadDataBackup()
	receivedMap := initializeDataBackup(receivedMsgs, receivedBkpMode, backupCallbackByInput)

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
		receivedBkpMutex,
	)

	for input, mainCallback := range mainCallbackByInput {
		dataChannel, found := dataChannelByInput[input]

		if !found {
			log.Fatalf("Data channel not defined for input '%s'.", input)
		}

		go ProcessData(workersPool, dataChannel, mainCallback, receivedMap, receivedBkpMode, receivedBkpMutex, procWgsByDataset, procWgsMutex)
	}

	// Waiting for close procedure.
	connWg.Wait()
}
