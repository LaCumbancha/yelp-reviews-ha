package main

import (
	"fmt"
	"time"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/LaCumbancha/yelp-review-ha/cmd/common/utils"
	"github.com/LaCumbancha/yelp-review-ha/cmd/nodes/inputs/reviews-scatter/core"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/yelp-review-ha/cmd/common/logger"
	props "github.com/LaCumbancha/yelp-review-ha/cmd/common/properties"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the RVWSCA_ prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix(props.InputI2_Name)

	// Add env variables supported
	configEnv.BindEnv("reviews", "data")
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("bulk", "size")
	configEnv.BindEnv("workers", "pool")
	configEnv.BindEnv("funbiz", "mappers")
	configEnv.BindEnv("weekdays", "mappers")
	configEnv.BindEnv("hashes", "mappers")
	configEnv.BindEnv("users", "mappers")
	configEnv.BindEnv("stars", "mappers")
	configEnv.BindEnv("monitors")
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

	rabbitIp := utils.GetConfigString(configEnv, configFile, "rabbitmq_ip")
	rabbitPort := utils.GetConfigString(configEnv, configFile, "rabbitmq_port")
	bulkSize := utils.GetConfigInt(configEnv, configFile, "bulk_size")
	funbizMappers := utils.GetConfigInt(configEnv, configFile, "funbiz_mappers")
	weekdaysMappers := utils.GetConfigInt(configEnv, configFile, "weekdays_mappers")
	hashesMappers := utils.GetConfigInt(configEnv, configFile, "hashes_mappers")
	usersMappers := utils.GetConfigInt(configEnv, configFile, "users_mappers")
	starsMappers := utils.GetConfigInt(configEnv, configFile, "stars_mappers")
	monitors := utils.GetConfigString(configEnv, configFile, "monitors")

	scatterConfig := core.ScatterConfig {
		RabbitIp:				rabbitIp,
		RabbitPort:				rabbitPort,
		BulkSize:				bulkSize,
		FunbizMappers:			funbizMappers,
		WeekdaysMappers:		weekdaysMappers,
		HashesMappers:			hashesMappers,
		UsersMappers:			usersMappers,
		StarsMappers:			starsMappers,
		Monitors:				monitors,
	}

	// Initializing custom logger.
	logBulkRate := utils.GetConfigInt(configEnv, configFile, "log_bulk_rate")
	logb.Instance().SetBulkRate(logBulkRate)

	// Waiting for all other nodes to correctly configure before starting sending reviews.
	scatter := core.NewScatter(scatterConfig)
	time.Sleep(2000 * time.Millisecond)

	scatter.Run()
	scatter.Stop()
}
