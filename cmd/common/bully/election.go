package bully

import (
	"fmt"
	"sync"
	"bytes"
	"net/http"
	"encoding/json"
	"github.com/LaCumbancha/yelp-review-ha/cmd/common/utils"

	log "github.com/sirupsen/logrus"
)

func Election(instance string, monitors []string, leader *string, leaderMutex *sync.Mutex) {
	iAmNewLeader := startElection(instance, monitors)

	if iAmNewLeader {
		log.Infof("Monitor detected as new leader. Starting announce.")

		/* It's important to first announce new leadership.
		 * This way, if there's another node which thinks it should be the leader, it can immediately start new election, and this node won't be leader then.
		 * */
		announceNewLeadership(instance, monitors)
		leaderMutex.Lock()
		*leader = instance
		leaderMutex.Unlock()
	}
}

/* In the election process, one sends an election msg to all bigger nodes in the system.
 * If any of them responds, it will continue the election process.
 * If none of them respond, then the current node is the new leader.
 * The return statement is the negation of the bool because if no one has answered, then this node is the leader.
 */
func startElection(instance string, monitors []string) bool {
	log.Debugf("Starting election.")

	biggerNodeResponded := false
	for _, monitor := range monitors {
		if utils.MonitorName(instance) < monitor {
			log.Tracef("In election with '%s'.", monitor)

			url := electionUrl(monitor)
			log.Tracef("Sending election message to '%s'.", monitor)
			_, err := http.Get(url)
			if err != nil {
				log.Warnf("Service '%s' detected as not running. Err: %s", monitor, err)
			} else {
				// If a bigger node responded, then there's no need to send extra messages to other bigger nodes because it will continue with the election anyway.
				biggerNodeResponded = true
				break
			}
		}
	}
	return !biggerNodeResponded
}

func announceNewLeadership(instance string, monitors []string) {
	msgJson, err := json.Marshal(leaderMessage(instance))
	if err != nil {
		log.Errorf("Error serializing leader message. Err: %s", err)
	} else {
		for _, monitor := range monitors {
			url := leaderUrl(monitor)
			log.Debugf("Announcing new leader to service '%s'.", monitor)

			if _, err := http.Post(url, "application/json", bytes.NewBuffer(msgJson)); err != nil {
				if utils.IsNoSuchHost(err) {
					log.Errorf("Error sending leader message to service '%s'. Couldn't find host.", monitor)
				} else {
					log.Errorf("Error sending leader message to service '%s'. Err: %s", monitor, err)
				}
			}
		}
	}
}

func leaderUrl(service string) string {
	return fmt.Sprintf("http://%s:%s/%s", service, BullyPort, LeaderEndpoint)
}

func electionUrl(service string) string {
	return fmt.Sprintf("http://%s:%s/%s", service, BullyPort, ElectionEndpoint)
}
