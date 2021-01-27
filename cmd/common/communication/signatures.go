package communication

import (
	"strings"
	"strconv"
)

const SIGNATURE_DELIMITER = "|"
const INNER_SIGNATURE_DELIMITER = "."

// The signature for the message "¡Hola Miguel!" from the dataset number 1, scatter instance number 0
// and bulk number 20 will be: "B.1.0.20|¡Hola Miguel!"
func SignMessage(datasetNumber int, instance string, bulkNumber int, message string) string {
	return "B" + INNER_SIGNATURE_DELIMITER + strconv.Itoa(datasetNumber) + INNER_SIGNATURE_DELIMITER + 
		instance + INNER_SIGNATURE_DELIMITER + strconv.Itoa(bulkNumber) + SIGNATURE_DELIMITER + message
}

func UnsignMessage(message string) (int, string, int, string) {
	idx1 := strings.Index(message, ".")
	if idx1 < 0 {
		return -1, "", -1, ""
	}
	
	idx2 := strings.Index(message[idx1+1:], ".")
	if idx2 < 0 {
		return -1, "", -1, ""
	}
	
	idx3 := strings.Index(message[idx1+idx2+2:], ".")
	if idx3 < 0 {
		return -1, "", -1, ""
	}
	
	idx4 := strings.Index(message[idx1+idx2+idx3+3:], "|")
	if idx4 < 0 {
		return -1, "", -1, ""
	}

	datasetNumber, err := strconv.Atoi(message[idx1+1:idx1+idx2+1])
	if err != nil {
		return -1, "", -1, ""
	}

	bulkNumber, err := strconv.Atoi(message[idx1+idx2+idx3+3:idx1+idx2+idx3+idx4+3])
	if err != nil {
		return -1, "", -1, ""
	}

	instance := message[idx1+idx2+2:idx1+idx2+idx3+2]
	data := message[idx1+idx2+idx3+idx4+4:]

	return datasetNumber, instance, bulkNumber, data
}