package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/joiners/funny-city/core"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
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
	configEnv.BindEnv("funcit", "aggregators")
	configEnv.BindEnv("log", "bulk", "rate")
	configEnv.BindEnv("output", "bulk", "size")
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
	log.SetLevel(log.InfoLevel)
	configEnv, configFile, err := InitConfig()

	if err != nil {
		log.Fatalf("Fatal error loading configuration. Err: '%s'", err)
	}

	instance := utils.GetConfigString(configEnv, configFile, "instance")
	rabbitIp := utils.GetConfigString(configEnv, configFile, "rabbitmq_ip")
	rabbitPort := utils.GetConfigString(configEnv, configFile, "rabbitmq_port")
	workersPool := utils.GetConfigInt(configEnv, configFile, "workers_pool")
	inputTopic := utils.GetConfigString(configEnv, configFile, "input_topic")
	funbizAggregators := utils.GetConfigInt(configEnv, configFile, "funbiz_aggregators")
	citbizMappers := utils.GetConfigInt(configEnv, configFile, "citbiz_mappers")
	funcitAggregators := utils.GetConfigInt(configEnv, configFile, "funcit_aggregators")
	outputBulkSize := utils.GetConfigInt(configEnv, configFile, "output_bulk_size")

	joinerConfig := core.JoinerConfig {
		Instance:				instance,
		RabbitIp:				rabbitIp,
		RabbitPort:				rabbitPort,
		WorkersPool:			workersPool,
		InputTopic: 			inputTopic,
		FunbizAggregators:		funbizAggregators,
		CitbizMappers:			citbizMappers,
		FuncitAggregators:		funcitAggregators,
		OutputBulkSize:			outputBulkSize,
	}

	// Initializing custom logger.
	logBulkRate := utils.GetConfigInt(configEnv, configFile, "log_bulk_rate")
	logb.Instance().SetBulkRate(logBulkRate)

	// Initializing joiner.
	joiner := core.NewJoiner(joinerConfig)
	joiner.Run()
	joiner.Stop()
}
