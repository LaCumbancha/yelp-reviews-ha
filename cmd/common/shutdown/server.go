package shutdown

import (
	"net/http"
	log "github.com/sirupsen/logrus"
)

func InitializeShutdownServer(shutdownHandler http.HandlerFunc) {
	http.HandleFunc("/" + ShutdownEndpoint, shutdownHandler)

	if err := http.ListenAndServe(":" + ShutdownPort, nil); err != nil {
		log.Fatalf("Error listening to shutdown port (%s). Err: %s", ShutdownPort, err)
	} else {
		log.Debugf("Shutdown server initialized at port %s.", ShutdownPort)
	}
}
