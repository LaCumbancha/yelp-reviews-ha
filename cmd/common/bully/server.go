package bully

import (
	"sync"
	"net/http"
	"encoding/json"

	log "github.com/sirupsen/logrus"
)

func InitializeBullyServer(instance string, monitors []string, leader *string, leaderMutex *sync.Mutex) {
	http.HandleFunc("/" + ElectionEndpoint, electionHandler(instance, monitors, leader, leaderMutex))
	http.HandleFunc("/" + LeaderEndpoint, leaderHandler(instance, monitors, leader, leaderMutex))

	if err := http.ListenAndServe(":" + BullyPort, nil); err != nil {
		log.Fatalf("Error listening to bully port. Err: %s", err)
	} else {
		log.Debugf("Bully server initialized")
	}
}

func electionHandler(instance string, monitors []string, leader *string, leaderMutex *sync.Mutex) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		log.Tracef("Election message received.")
		Election(instance, monitors, leader, leaderMutex)
	}
}

func leaderHandler(instance string, monitors []string, leader *string, leaderMutex *sync.Mutex) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		log.Tracef("Leader message received.")
		
		var response LeaderResponse
		err := json.NewDecoder(request.Body).Decode(&response)

		// Try to decode the request body into the struct. If there is an error, respond to the client with the error message and a 400 status code.
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}

		log.Debugf("New leader received: Monitor %s.", response.Leader)
		leaderMutex.Lock()
		*leader = response.Leader
		leaderMutex.Unlock()

		if response.Leader < instance {
			log.Debugf("New leader message received from a smaller monitor. Re-throwing election.")
			Election(instance, monitors, leader, leaderMutex)
		}
	}
}
