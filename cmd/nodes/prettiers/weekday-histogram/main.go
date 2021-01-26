package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/prettiers/weekday-histogram/core"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the HISTOPRE prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("histopre")

	// Add env variables supported
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("workers", "pool")
	configEnv.BindEnv("weekday", "aggregators")
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
	workersPool := utils.GetConfigInt(configEnv, configFile, "workers_pool")
	weekdayAggregators := utils.GetConfigInt(configEnv, configFile, "weekday_aggregators")

	prettierConfig := core.PrettierConfig {
		RabbitIp:				rabbitIp,
		RabbitPort:				rabbitPort,
		WorkersPool:			workersPool,
		WeekdayAggregators:		weekdayAggregators,
	}

	// Custom logger initialization.
	logb.Instance().SetBulkRate(1)

	prettier := core.NewPrettier(prettierConfig)
	prettier.Run()
	prettier.Stop()
}
