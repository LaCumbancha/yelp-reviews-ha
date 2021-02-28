package healthcheck

import (
	"net/http"
	"encoding/json"
	log "github.com/sirupsen/logrus"
)

func InitializeHealthcheckServer() {
	http.HandleFunc("/" + HealthCheckEndpoint, HealthCheckHandler)

	if err := http.ListenAndServe(":" + HealthCheckPort, nil); err != nil {
		log.Fatalf("Error listeing to health-check port (%s). Err: %s", HealthCheckPort, err)
	} else {
		log.Debugf("Health-Check server initialized at port %s.", HealthCheckPort)
	}
}

func HealthCheckHandler(writer http.ResponseWriter, request *http.Request) {
	bytes, err := json.Marshal(HealthCheckResponse)
	if err != nil {
		log.Errorf("Error serializing health-check response. Err: %s", err)
	}

	writer.WriteHeader(HealthCheckStatusCode)
	writer.Write(bytes)
}