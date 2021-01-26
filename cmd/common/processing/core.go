package processing

import (
	"sync"
	"github.com/streadway/amqp"
)

// Common processing for Mappers, Filters, Aggregators and Prettiers.
func Transformation(
	workersPool int,
	endSignals int,
	inputs <- chan amqp.Delivery,
	mainCallback func(int, string),
	finishCallback func(int),
	closeCallback func(),
) {
	mainChannel := make(chan amqp.Delivery)

	closingConn := false
	connMutex := &sync.Mutex{}

	var connWg sync.WaitGroup
	connWg.Add(1)

	initialProcWait := 1
	var procWg sync.WaitGroup
	procWg.Add(initialProcWait)

	go InitializeProcessingWorkers(workersPool, mainChannel, mainCallback, &procWg)
	go ProcessInputs(inputs, mainChannel, endSignals, &procWg, &connWg)
	go ProcessFinish(finishCallback, &procWg, initialProcWait, closingConn, connMutex)
	CloseConnection(closeCallback, &procWg, &connWg, closingConn, connMutex)
}

// Common processing for Joiners.
func Join(
	workersPool int,
	endSignals1 int,
	endSignals2 int,
	inputs1 <- chan amqp.Delivery,
	inputs2 <- chan amqp.Delivery,
	mainCallback1 func(int, string),
	mainCallback2 func(int, string),
	finishCallback func(int),
	closeCallback func(),
	reloadWaitCount int,
) {
	var connWg sync.WaitGroup
	connWg.Add(1)

	var procWg sync.WaitGroup
	procWg.Add(2)

	closingConn := false
	connMutex := &sync.Mutex{}

	mainChannel1 := make(chan amqp.Delivery)
	mainChannel2 := make(chan amqp.Delivery)
	
	go InitializeProcessingWorkers(int(workersPool/2), mainChannel1, mainCallback1, &procWg)
	go ProcessInputs(inputs1, mainChannel1, endSignals1, &procWg, &connWg)	

	go InitializeProcessingWorkers(int(workersPool/2), mainChannel2, mainCallback2, &procWg)
	go ProcessInputs(inputs2, mainChannel2, endSignals2, &procWg, &connWg)

	// Retrieving joined data and closing connection.
	go ProcessFinish(finishCallback, &procWg, reloadWaitCount, closingConn, connMutex)
	CloseConnection(closeCallback, &procWg, &connWg, closingConn, connMutex)
}
