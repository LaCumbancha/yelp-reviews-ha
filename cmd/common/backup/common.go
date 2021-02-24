package backup

import (
	"fmt"
	"strings"
	"strconv"

	log "github.com/sirupsen/logrus"
)

type BackupType string
const StartBkp = "Starting-Signals"
const FinishBkp = "Finishing-Signals"
const CloseBkp = "Closing-Signals"
const DataBkp = "Data"

type BackupMode string
const IdBackup = "ID"
const FullBackup = "FULL"

const BkpMainPath = "/bkps"
const StartPath = "starting"
const FinishPath = "finishing"
const ClosePath = "closing"
const DataPath = "data"

const FileKey1 = "1"
const FileKey2 = "2"

func calculateBackupPath(bkpType BackupType) string {
	var path string

	switch bkpType {
	case StartBkp:
		path = BkpMainPath + "/" + StartPath
	case FinishBkp:
		path = BkpMainPath + "/" + FinishPath
	case CloseBkp:
		path = BkpMainPath + "/" + ClosePath
	case DataBkp:
		path = BkpMainPath + "/" + DataPath
	default:
		log.Fatalf("Unexpected backup type: %s.", bkpType)
	}

	return path
}

type DataBackup struct {
	Signature	string
	Data		string
}

func packBackup(messageId string, data string) string {
	return fmt.Sprintf("%s|%s", messageId, data)
}

func unpackBackup(backup string) DataBackup {
	idx1 := strings.Index(backup, "|")
	if idx1 < 0 {
		return DataBackup { Signature: "", Data: "" }
	}

	return DataBackup { Signature: backup[:idx1], Data: backup[idx1+1:] }
}

func datasetFromBackupDirectory(directoryName string) int {
	idx1 := strings.Index(directoryName, ".")
	if idx1 < 0 {
		return -1
	}

	dataset, err := strconv.Atoi(directoryName[idx1+1:])
	if err != nil {
		return -1
	}

	return dataset
}