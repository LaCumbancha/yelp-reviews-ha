package utils

import (
	"strconv"
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

func IntInSlice(aInt int, aSlice []int) bool {
    for _, value := range aSlice {
        if aInt == value {
            return true
        }
    }
    return false
}
