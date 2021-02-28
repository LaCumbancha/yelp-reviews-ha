package docker

import (
	"context"
	"github.com/docker/docker/client"
	"github.com/docker/docker/api/types"

	log "github.com/sirupsen/logrus"
)

func StartService(service string) {
	log.Infof("Starting service '%s'.", service)

	ctx := context.Background()
	client, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Errorf("Error initializing Docker client while starting service '%s'. Err: %s", service, err)
	}

	if err = client.ContainerStart(ctx, service, types.ContainerStartOptions{}); err != nil {
		log.Errorf("Error starting service '%s'. Err: %s", service, err)
	}
}
