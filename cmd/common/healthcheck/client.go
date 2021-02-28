package healthcheck

import (
	"fmt"
	"net/http"
	"github.com/LaCumbancha/yelp-review-ha/cmd/common/utils"
	log "github.com/sirupsen/logrus"
)

func healthCheckPath(service string) string {
	return fmt.Sprintf("http://%s:%s/%s", service, HealthCheckPort, HealthCheckEndpoint)
}

func HealthCheckControl(service string) bool {
	if response, err := http.Get(healthCheckPath(service)); err != nil {
		errTxt := fmt.Sprintf("Err: %s", err)
		if utils.IsNoSuchHost(err) {
			errTxt = "Couldn't find host."
		}
		if utils.IsConnectionRefused(err) {
			errTxt = "Connection refused."
		}
		log.Errorf("Error executing GET to service '%s' health-check. %s", service, errTxt)
		return false
	} else {
		return response.StatusCode == HealthCheckStatusCode
	}
}
