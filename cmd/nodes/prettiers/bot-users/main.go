package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/LaCumbancha/yelp-review-ha/cmd/common/utils"
	"github.com/LaCumbancha/yelp-review-ha/cmd/nodes/prettiers/bot-users/core"

	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/yelp-review-ha/cmd/common/backup"
	logb "github.com/LaCumbancha/yelp-review-ha/cmd/common/logger"
	props "github.com/LaCumbancha/yelp-review-ha/cmd/common/properties"
	health "github.com/LaCumbancha/yelp-review-ha/cmd/common/healthcheck"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the BESTUSER prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix(props.PrettierP3_Name)

	// Add env variables supported
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("workers", "pool")
	configEnv.BindEnv("min", "reviews")
	configEnv.BindEnv("bots", "aggregators")
	configEnv.BindEnv("log", "level")
	configEnv.BindEnv("config", "file")

	// Read config file if it's present
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

	bkp.InitializeBackupStructure()

	rabbitIp := utils.GetConfigString(configEnv, configFile, "rabbitmq_ip")
	rabbitPort := utils.GetConfigString(configEnv, configFile, "rabbitmq_port")
	workersPool := utils.GetConfigInt(configEnv, configFile, "workers_pool")
	minReviews := utils.GetConfigInt(configEnv, configFile, "min_reviews")
	botsAggregators := utils.GetConfigInt(configEnv, configFile, "bots_aggregators")

	prettierConfig := core.PrettierConfig {
		RabbitIp:				rabbitIp,
		RabbitPort:				rabbitPort,
		WorkersPool:			workersPool,
		MinReviews:				minReviews,
		BotsAggregators:		botsAggregators,
	}

	// Custom logger initialization.
	logb.Instance().SetBulkRate(1)

	go health.InitializeHealthcheckServer()

	prettier := core.NewPrettier(prettierConfig)
	prettier.Run()
	prettier.Stop()
}
