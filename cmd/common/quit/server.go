package quit

import (
	"net/http"
	log "github.com/sirupsen/logrus"
)

func InitializeShutdownServer(stopSubHandler func(), shutdownSubHandler func()) {
	http.HandleFunc("/" + StopEndpoint, stopHandler(stopSubHandler))
	http.HandleFunc("/" + ShutdownEndpoint, shutdownHandler(shutdownSubHandler))

	if err := http.ListenAndServe(":" + QuitPort, nil); err != nil {
		log.Fatalf("Error listening to graceful quit port (%s). Err: %s", QuitPort, err)
	} else {
		log.Debugf("Graceful quit server initialized at port %s.", QuitPort)
	}
}

func stopHandler(stopSubHandler func()) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request){ 
		stopSubHandler()
		writer.WriteHeader(GracefulQuitStatusCode)
	}
}

func shutdownHandler(shutdownSubHandler func()) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request){ 
		shutdownSubHandler()
		writer.WriteHeader(GracefulQuitStatusCode)
	}
}