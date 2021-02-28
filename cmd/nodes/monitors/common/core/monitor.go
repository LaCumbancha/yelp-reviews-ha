package core

import (
	"sync"
	"strings"
	"github.com/jasonlvhit/gocron"
	"github.com/LaCumbancha/yelp-review-ha/cmd/common/utils"

	log "github.com/sirupsen/logrus"
	quit "github.com/LaCumbancha/yelp-review-ha/cmd/common/quit"
	bully "github.com/LaCumbancha/yelp-review-ha/cmd/common/bully"
	docker "github.com/LaCumbancha/yelp-review-ha/cmd/common/docker"
	health "github.com/LaCumbancha/yelp-review-ha/cmd/common/healthcheck"
)

type MonitorConfig struct {
	Instance 			string
	CheckInterval 		int
	ObserverNodes		string
	ObservableNodes		string
}

type Monitor struct {
	instance 			string
	checkInterval 		int
	observerNodes 		[]string
	observableNodes 	[]string
	leader 				string
	leaderMutex			*sync.Mutex
	running 			bool
	runningMutex		*sync.Mutex
}

func NewMonitor(config MonitorConfig) *Monitor {
	monitor := &Monitor {
		instance:			config.Instance,
		checkInterval:		config.CheckInterval,
		observerNodes:		strings.Split(config.ObserverNodes, ","),
		observableNodes:	strings.Split(config.ObservableNodes, ","),
		leaderMutex:		&sync.Mutex{},
		running:			true,
		runningMutex:		&sync.Mutex{},
	}

	return monitor
}

func (monitor *Monitor) Run() {
	log.Infof("Starting monitoring system nodes.")

	finishWg := &sync.WaitGroup{}

	finishWg.Add(1)
	go quit.InitializeShutdownServer(monitor.stopHandler(), monitor.shutdownHandler(finishWg))

	go bully.InitializeBullyServer(monitor.instance, monitor.observerNodes, &monitor.leader, monitor.leaderMutex)
	bully.Election(monitor.instance, monitor.observerNodes, &monitor.leader, monitor.leaderMutex)

	gocron.Start()
	gocron.Every(uint64(monitor.checkInterval)).Second().Do(monitor.routineCheck)

	finishWg.Wait()
}

func (monitor *Monitor) stopHandler() func() {
	return func(){ 
		monitor.runningMutex.Lock()
		monitor.running = false
		monitor.runningMutex.Unlock()
		log.Infof("Monitor stopped.")
	}
}

func (monitor *Monitor) shutdownHandler(finishWg *sync.WaitGroup) func() {
	return func(){ 
		finishWg.Done()
		log.Infof("Monitor shutdown started.")
	}
}

func (monitor *Monitor) routineCheck() {
	monitor.runningMutex.Lock()
	
	if monitor.running {

		monitor.leaderMutex.Lock()
		currentlyLeadering := monitor.leader == monitor.instance
		monitor.leaderMutex.Unlock()
	
		if currentlyLeadering {
			monitor.leaderRoutineCheck()
		} else {
			monitor.nonLeaderRoutineCheck()
		}

	}

	monitor.runningMutex.Unlock()
}

func (monitor *Monitor) leaderRoutineCheck() {
	for _, service := range monitor.observableNodes {
		log.Debugf("Checking service '%s' status.", service)
		isServiceAlive := health.HealthCheckControl(service)

		if !isServiceAlive {
			log.Warnf("Service '%s' detected as not running.", service)
			docker.StartService(service)
		}
	}
}

func (monitor *Monitor) nonLeaderRoutineCheck() {
	isLeaderAlive := health.HealthCheckControl(utils.MonitorName(monitor.leader))

	if !isLeaderAlive {
		log.Warnf("Monitor %s (current leader) detected as not running. Throwing election.", monitor.leader)
		bully.Election(monitor.instance, monitor.observerNodes, &monitor.leader, monitor.leaderMutex)
	}
}

func (monitor *Monitor) Stop() {
	log.Infof("Closing Monitor.")
}
