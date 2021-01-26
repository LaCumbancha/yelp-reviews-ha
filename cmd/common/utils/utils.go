package utils

import (
	"strconv"
	log "github.com/sirupsen/logrus"
)

func GeneratePartitionMap(partitions int, partitionableValues []string) map[string]string {
	partitionsMap := make(map[string]string)

	for idx, value := range partitionableValues {
		partitionsMap[value] = strconv.Itoa(idx % partitions)
	}

	return partitionsMap
}

func GetMapDistinctValues(aMap map[string]string) []string {
	auxMap := make(map[string]bool)

	for _, value := range aMap {
        auxMap[value] = true
    }

    values := make([]string, 0, len(auxMap))
    for key, _ := range auxMap {
    	values = append(values, key)
    }

    return values
}

func SetLogLevel(logLevel string) {
	switch logLevel {
	case "TRACE":
		log.SetLevel(log.TraceLevel)
	case "DEBUG":
		log.SetLevel(log.DebugLevel)
	case "INFO":
		log.SetLevel(log.InfoLevel)
	case "WARN":
		log.SetLevel(log.WarnLevel)
	case "ERROR":
		log.SetLevel(log.ErrorLevel)
	}

}