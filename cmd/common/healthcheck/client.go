package healthcheck

import (
	"fmt"
	"net/http"
	log "github.com/sirupsen/logrus"
)

func healthCheckPath(service string) string {
	return fmt.Sprintf("http://%s:%s/%s", service, HealthCheckPort, HealthCheckEndpoint)
}

func HealthCheckControl(service string) bool {
	if response, err := http.Get(healthCheckPath(service)); err != nil {
		log.Errorf("Error executing GET to service %s health-check. Err: %s", service, err)
		return false
	} else {
		return response.StatusCode == HealthCheckStatusCode
	}
}
