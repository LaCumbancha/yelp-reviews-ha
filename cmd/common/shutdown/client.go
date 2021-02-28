package shutdown

import(
	"fmt"
	"bytes"
	"net/http"
	"github.com/LaCumbancha/yelp-review-ha/cmd/common/utils"

	log "github.com/sirupsen/logrus"
)

const retries = 5

func shutdownPath(service string) string {
	return fmt.Sprintf("http://%s:%s/%s", service, ShutdownPort, ShutdownEndpoint)
}

func ShutdownRequest(service string) {
	iteration := 1
	for iteration <= retries {
		if _, err := http.Post(shutdownPath(service), "application/json", bytes.NewBuffer([]byte(""))); err != nil {
			errTxt := fmt.Sprintf("Err: %s", err)
			if utils.IsNoSuchHost(err) {
				errTxt = "Couldn't find host."
			}
			if utils.IsConnectionRefused(err) {
				errTxt = "Connection refused."
			}
			log.Debugf("Error sending shutdown message to service '%s'. %s.", service, errTxt)
		} else {
			log.Infof("Shutdown message sent to service '%s'.", service)
			break
		}
		iteration++
	}

	if iteration > retries {
		log.Errorf("Couldn't send shutdown message to service '%s'.", service)
	}
}
