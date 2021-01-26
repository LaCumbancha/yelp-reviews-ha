package main

import (
	"fmt"
	"time"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/inputs/business-scatter/core"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the RVWSCA_ prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("bizsca")

	// Add env variables supported
	configEnv.BindEnv("business", "data")
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("bulk", "size")
	configEnv.BindEnv("workers", "pool")
	configEnv.BindEnv("citbiz", "mappers")
	configEnv.BindEnv("log", "bulk", "rate")
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

	businessData := utils.GetConfigString(configEnv, configFile, "business_data")
	rabbitIp := utils.GetConfigString(configEnv, configFile, "rabbitmq_ip")
	rabbitPort := utils.GetConfigString(configEnv, configFile, "rabbitmq_port")
	bulkSize := utils.GetConfigInt(configEnv, configFile, "bulk_size")
	workersPool := utils.GetConfigInt(configEnv, configFile, "workers_pool")
	citbizMappers := utils.GetConfigInt(configEnv, configFile, "citbiz_mappers")

	scatterConfig := core.ScatterConfig {
		Data:					businessData,
		RabbitIp:				rabbitIp,
		RabbitPort:				rabbitPort,
		BulkSize:				bulkSize,
		WorkersPool:			workersPool,
		CitbizMappers:			citbizMappers,
	}

	// Initializing custom logger.
	logBulkRate := int(utils.GetConfigInt(configEnv, configFile, "log_bulk_rate")/5)
	logb.Instance().SetBulkRate(logBulkRate)

	// Waiting for all other nodes to correctly configure before starting sending reviews.
	scatter := core.NewScatter(scatterConfig)
	time.Sleep(2000 * time.Millisecond)
	scatter.Run()
	scatter.Stop()
}
