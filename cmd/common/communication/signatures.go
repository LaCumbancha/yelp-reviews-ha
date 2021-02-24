package communication

import (
	"strings"
	"strconv"
)

const SIGNATURE_DELIMITER = "|"
const INNER_SIGNATURE_DELIMITER = "."


// The signature for a message from the node F3, dataset number 1, instance number 0 and bulk number 20 will be: "F3.1.0.20"
func MessageSignature(nodeCode string, dataset int, instance string, bulk int) string {
	return nodeCode + INNER_SIGNATURE_DELIMITER + strconv.Itoa(dataset) + INNER_SIGNATURE_DELIMITER + 
		instance + INNER_SIGNATURE_DELIMITER + strconv.Itoa(bulk)
}

func SignatureData(signature string) (string, int, string, int) {
	idx1 := strings.Index(signature, ".")
	if idx1 < 0 {
		return "", -1, "", -1
	}
	
	idx2 := strings.Index(signature[idx1+1:], ".")
	if idx2 < 0 {
		return "", -1, "", -1
	}
	
	idx3 := strings.Index(signature[idx1+idx2+2:], ".")
	if idx3 < 0 {
		return "", -1, "", -1
	}

	dataset, err := strconv.Atoi(signature[idx1+1:idx1+idx2+1])
	if err != nil {
		return "", -1, "", -1
	}

	bulk, err := strconv.Atoi(signature[idx1+idx2+idx3+3:])
	if err != nil {
		return "", -1, "", -1
	}

	nodeCode := signature[:idx1]
	instance := signature[idx1+idx2+2:idx1+idx2+idx3+2]

	return nodeCode, dataset, instance, bulk
}

func SignMessage(nodeCode string, dataset int, instance string, bulk int, message string) string {
	return MessageSignature(nodeCode, dataset, instance, bulk) + SIGNATURE_DELIMITER + message
}

func UnsignMessage(message string) (string, int, string, int, string) {
	idx := strings.Index(message, "|")
	if idx < 0 {
		return "", -1, "", -1, ""
	}

	nodeCode, dataset, instance, bulk := SignatureData(message[:idx])
	return nodeCode, dataset, instance, bulk, message[idx+1:]
}
