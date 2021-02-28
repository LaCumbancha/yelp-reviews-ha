package shutdown

import(
	"fmt"
	"bytes"
	"net/http"
	"github.com/LaCumbancha/yelp-review-ha/cmd/common/utils"

	log "github.com/sirupsen/logrus"
)

func shutdownPath(service string) string {
	return fmt.Sprintf("http://%s:%s/%s", service, ShutdownPort, ShutdownEndpoint)
}

func ShutdownRequest(service string) {
	if _, err := http.Post(shutdownPath(service), "application/json", bytes.NewBuffer([]byte(""))); err != nil {
		errTxt := fmt.Sprintf("Err: %s", err)
		if utils.IsNoSuchHost(err) {
			errTxt = "Couldn't find host."
		}
		if utils.IsConnectionRefused(err) {
			errTxt = "Connection refused."
		}
		log.Errorf("Error sending shutdown message to service '%s'. %s.", service, errTxt)
	}
}
