package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/LaCumbancha/yelp-review-ha/cmd/common/utils"
	"github.com/LaCumbancha/yelp-review-ha/cmd/nodes/joiners/funny-city/core"

	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/yelp-review-ha/cmd/common/backup"
	logb "github.com/LaCumbancha/yelp-review-ha/cmd/common/logger"
	health "github.com/LaCumbancha/yelp-review-ha/cmd/common/healthcheck"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the FUNCITJOIN prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("funcitjoin")

	// Add env variables supported
	configEnv.BindEnv("instance")
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("workers", "pool")
	configEnv.BindEnv("input", "topic")
	configEnv.BindEnv("funbiz", "aggregators")
	configEnv.BindEnv("citbiz", "mappers")
	configEnv.BindEnv("funcit", "tops")
	configEnv.BindEnv("log", "bulk", "rate")
	configEnv.BindEnv("output", "bulk", "size")
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

	instance := utils.GetConfigString(configEnv, configFile, "instance")
	rabbitIp := utils.GetConfigString(configEnv, configFile, "rabbitmq_ip")
	rabbitPort := utils.GetConfigString(configEnv, configFile, "rabbitmq_port")
	workersPool := utils.GetConfigInt(configEnv, configFile, "workers_pool")
	inputTopic := utils.GetConfigString(configEnv, configFile, "input_topic")
	funbizAggregators := utils.GetConfigInt(configEnv, configFile, "funbiz_aggregators")
	citbizMappers := utils.GetConfigInt(configEnv, configFile, "citbiz_mappers")
	funcitTops := utils.GetConfigInt(configEnv, configFile, "funcit_tops")
	outputBulkSize := utils.GetConfigInt(configEnv, configFile, "output_bulk_size")

	joinerConfig := core.JoinerConfig {
		Instance:				instance,
		RabbitIp:				rabbitIp,
		RabbitPort:				rabbitPort,
		WorkersPool:			workersPool,
		InputTopic: 			inputTopic,
		FunbizAggregators:		funbizAggregators,
		CitbizMappers:			citbizMappers,
		FuncitTops:				funcitTops,
		OutputBulkSize:			outputBulkSize,
	}

	// Initializing custom logger.
	logBulkRate := int(utils.GetConfigInt(configEnv, configFile, "log_bulk_rate")/5)
	if logBulkRate < 1 { logBulkRate = 1 }
	logb.Instance().SetBulkRate(logBulkRate)

	go health.InitializeHealthcheckServer()

	// Initializing joiner.
	joiner := core.NewJoiner(joinerConfig)
	joiner.Run()
	joiner.Stop()
}
