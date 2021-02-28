package main

import (
	"fmt"
	"time"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/LaCumbancha/yelp-review-ha/cmd/common/utils"
	"github.com/LaCumbancha/yelp-review-ha/cmd/nodes/monitors/common/core"

	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/yelp-review-ha/cmd/common/backup"
	props "github.com/LaCumbancha/yelp-review-ha/cmd/common/properties"
	health "github.com/LaCumbancha/yelp-review-ha/cmd/common/healthcheck"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the node prefix.
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix(props.Monitor_Name)

	// Add env variables supported.
	configEnv.BindEnv("instance")
	configEnv.BindEnv("check", "interval")
	configEnv.BindEnv("observer", "nodes")
	configEnv.BindEnv("observable", "nodes")
	configEnv.BindEnv("log", "level")
	configEnv.BindEnv("config", "file")

	// Read config file if it's present.
	var configFile = viper.New()
	if configFileName := configEnv.GetString("config_file"); configFileName != "" {
		path, file, ctype := utils.GetConfigFile(configFileName)

		configFile.SetConfigName(file)
		configFile.SetConfigType(ctype)
		configFile.AddConfigPath(path)
		err := configFile.ReadInConfig()

		if err != nil {
			return nil, nil, errors.Wrapf(err, fmt.Sprintf("Couldn't load config file"))
		}
	}

	return configEnv, configFile, nil
}

func main() {
	configEnv, configFile, err := InitConfig()

	if err != nil {
		log.Fatalf("Fatal error loading configuration. Err: '%s'", err)
	}

	logLevel := utils.GetConfigString(configEnv, configFile, "log_level")
	utils.SetLogLevel(logLevel)

	bkp.InitializeMonitorBackup(core.Running)

	instance := utils.GetConfigString(configEnv, configFile, "instance")
	checkInterval := utils.GetConfigInt(configEnv, configFile, "check_interval")
	observerNodes := utils.GetConfigString(configEnv, configFile, "observer_nodes")
	observableNodes := utils.GetConfigString(configEnv, configFile, "observable_nodes")

	monitorConfig := core.MonitorConfig {
		Instance:				instance,
		CheckInterval:			checkInterval,
		ObserverNodes:			observerNodes,
		ObservableNodes:		observableNodes,
	}

	go health.InitializeHealthcheckServer()

	time.Sleep(2 * time.Second)

	// Initializing monitor.
	monitor := core.NewMonitor(monitorConfig)
	monitor.Run()
	monitor.Stop()
}
