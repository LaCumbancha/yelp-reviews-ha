package quit

import(
	"fmt"
	"time"
	"bytes"
	"net/http"
	"github.com/LaCumbancha/yelp-review-ha/cmd/common/utils"

	log "github.com/sirupsen/logrus"
)

const retries = 5

func stopPath(service string) string {
	return fmt.Sprintf("http://%s:%s/%s", service, QuitPort, StopEndpoint)
}

func shutdownPath(service string) string {
	return fmt.Sprintf("http://%s:%s/%s", service, QuitPort, ShutdownEndpoint)
}

func retriesPolicy(executeRequestOk func() bool) bool {
	iteration := 1
	for iteration <= retries {
		if executeRequestOk() {
			return true
		}
		iteration++

		// Waiting for next retry to avoid network problems.
		time.Sleep(1 * time.Second)
	}

	return false
}

func StopRequest(service string) bool {
	return retriesPolicy(func() bool {
		if response, err := http.Post(stopPath(service), "application/json", bytes.NewBuffer([]byte(""))); err != nil {
			errText := utils.NetworkErrorText(err)
			log.Errorf("Error sending stop message to service '%s'. Err: %s", service, errText)
		} else if response.StatusCode != GracefulQuitStatusCode {
			log.Errorf("Error sending stop message to service '%s'. Response status code: %d.", service, response.StatusCode)
		} else {
			log.Infof("Stop message sent to service '%s'.", service)
			return true
		}

		return false
	})
}

func ShutdownRequest(service string) bool {
	return retriesPolicy(func() bool {
		if response, err := http.Post(shutdownPath(service), "application/json", bytes.NewBuffer([]byte(""))); err != nil {
			errText := utils.NetworkErrorText(err)
			log.Debugf("Error sending shutdown message to service '%s'. Err: %s", service, errText)
		} else if response.StatusCode != GracefulQuitStatusCode {
			log.Debugf("Error sending shutdown message to service '%s'. Response status code: %d.", service, response.StatusCode)
		} else {
			log.Infof("Shutdown message sent to service '%s'.", service)
			return true
		}

		return false
	})
}
