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

type MultiFlowBackup struct {
	Flow	int
	Data	string
}

func setFlowAndData(toBackup MultiFlowBackup) string {
	return fmt.Sprintf("%d||%s", toBackup.Flow, toBackup.Data)
}

func getFlowAndData(backup string) MultiFlowBackup {
	separator := strings.Index(backup, "|")
	if separator < 0 {
		return MultiFlowBackup { Flow: -1, Data: "" }
	}
	
	flow, err := strconv.Atoi(backup[:separator])
	if err != nil {
		return MultiFlowBackup { Flow: -1, Data: "" }
	}

	return MultiFlowBackup { Flow: flow, Data: backup[separator+1:] }
}
