package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/aggregators/user/core"

	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/reviews-analysis/cmd/common/backup"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the USERAGG prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("useragg")

	// Add env variables supported
	configEnv.BindEnv("instance")
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("workers", "pool")
	configEnv.BindEnv("input", "topic")
	configEnv.BindEnv("user", "mappers")
	configEnv.BindEnv("user", "filters")
	configEnv.BindEnv("botuser", "filters")
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
	userMappers := utils.GetConfigInt(configEnv, configFile, "user_mappers")
	userFilters := utils.GetConfigInt(configEnv, configFile, "user_filters")
	botUserFilters := utils.GetConfigInt(configEnv, configFile, "botuser_filters")
	outputBulkSize := utils.GetConfigInt(configEnv, configFile, "output_bulk_size")

	aggregatorConfig := core.AggregatorConfig {
		Instance:				instance,
		RabbitIp:				rabbitIp,
		RabbitPort:				rabbitPort,
		WorkersPool:			workersPool,
		InputTopic: 			inputTopic,
		UserMappers:			userMappers,
		UserFilters:			userFilters,
		BotUserFilters:			botUserFilters,
		OutputBulkSize:			outputBulkSize,
	}

	// Initializing custom logger.
	logBulkRate := utils.GetConfigInt(configEnv, configFile, "log_bulk_rate")
	logb.Instance().SetBulkRate(logBulkRate)

	// Initializing aggregator.
	aggregator := core.NewAggregator(aggregatorConfig)
	aggregator.Run()
	aggregator.Stop()
}
