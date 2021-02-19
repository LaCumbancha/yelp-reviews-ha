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
const ReceivedBkp = "Received-Messages"
const DataBkp = "Data"

const BkpMainPath = "/bkps"
const StartPath = "starting"
const FinishPath = "finishing"
const ClosePath = "closing"
const ReceivedPath = "received"
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
	case ReceivedBkp:
		path = BkpMainPath + "/" + ReceivedPath
	case DataBkp:
		path = BkpMainPath + "/" + DataPath
	default:
		log.Fatalf("Unexpected backup type: %s.", bkpType)
	}

	return path
}

type DataBackup struct {
	Dataset		int
	Flow		int
	Data		string
}

func packageBackupMessage(toBackup DataBackup) string {
	return fmt.Sprintf("%d|%d|%s", toBackup.Dataset, toBackup.Flow, toBackup.Data)
}

func unpackageBackupMessage(backup string) DataBackup {
	idx1 := strings.Index(backup, "|")
	if idx1 < 0 {
		return DataBackup { Dataset: -1, Flow: -1, Data: "" }
	}
	
	idx2 := strings.Index(backup[idx1+1:], "|")
	if idx2 < 0 {
		return DataBackup { Dataset: -1, Flow: -1, Data: "" }
	}

	dataset, err := strconv.Atoi(backup[:idx1])
	if err != nil {
		return DataBackup { Dataset: -1, Flow: -1, Data: "" }
	}

	flow, err := strconv.Atoi(backup[idx1+1:idx1+idx2+1])
	if err != nil {
		return DataBackup { Dataset: -1, Flow: -1, Data: "" }
	}

	return DataBackup { Dataset: dataset, Flow: flow, Data: backup[idx1+idx2+2:] }
}
