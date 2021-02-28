package core

import (
	"sync"
	"net/http"
	log "github.com/sirupsen/logrus"
)

func initializeMonitorServer(finishWg *sync.WaitGroup, leaderWg *sync.WaitGroup) {
	http.HandleFunc("/shutdown", shutdownHandler(finishWg, leaderWg))

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Error listeing to monitor port. Err: %s", err)
	} else {
		log.Debugf("Monitor server initialized")
	}
}

func shutdownHandler(finishWg *sync.WaitGroup, leaderWg *sync.WaitGroup) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request){
		leaderWg.Wait()
		finishWg.Done()
	}
}
