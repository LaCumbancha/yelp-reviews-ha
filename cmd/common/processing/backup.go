package processing

import (
	"os"
	"fmt"
	"io/ioutil"
	"encoding/json"

	log "github.com/sirupsen/logrus"
)

type BackupType string

const StartBkp = "Starting-Signals"
const FinishBkp = "Finishing-Signals"
const CloseBkp = "Closing-Signals"
const DataBkp = "Data"

const BkpMainPath = "/bkps"
const StartPath = "starting"
const FinishPath = "finishing"
const ClosePath = "closing"
const DataPath = "data"

func InitializeBackupStructure() {
	if _, err := os.Stat(BkpMainPath); os.IsNotExist(err) {
		log.Infof("Creating backup directories from scratch.")

		err := os.Mkdir(BkpMainPath, os.ModePerm)
		if err != nil {
			log.Fatalf("Error creating main backup folder. Err: '%s'", err)
		}

		err = os.MkdirAll(fmt.Sprintf("%s/%s", BkpMainPath, StartPath), os.ModePerm)
		if err != nil {
			log.Fatalf("Error creating starting signals backup folder. Err: '%s'", err)
		}
	
		err = os.MkdirAll(fmt.Sprintf("%s/%s", BkpMainPath, FinishPath), os.ModePerm)
		if err != nil {
			log.Fatalf("Error creating finishing signals backup folder. Err: '%s'", err)
		}
	
		err = os.MkdirAll(fmt.Sprintf("%s/%s", BkpMainPath, ClosePath), os.ModePerm)
		if err != nil {
			log.Fatalf("Error creating closing signals backup folder. Err: '%s'", err)
		}
	
		err = os.MkdirAll(fmt.Sprintf("%s/%s", BkpMainPath, DataPath), os.ModePerm)
		if err != nil {
			log.Fatalf("Error creating data backup folder. Err: '%s'", err)
		}	
	} else {
		log.Infof("Backup directory found.")
	}
}

func LoadBackup(bkpType BackupType) []byte {
	path := calculateBackupPath(bkpType)

	backupName1 := fmt.Sprintf("%s/bkp.1", path)
	backupName2 := fmt.Sprintf("%s/bkp.2", path)

	_, err1 := os.Stat(backupName1)
	_, err2 := os.Stat(backupName2)

	if os.IsNotExist(err1) && os.IsNotExist(err2) {
		log.Warnf("No %s backup file found. Setting empty backup as default.", bkpType)
		return nil
	}

	jsonFile, err := os.Open(backupName1)
	if err != nil {
		log.Warnf("Error opening %s backup file #1. Err: '%s'", bkpType, err)
	} else {
		bytes, err := ioutil.ReadAll(jsonFile)
		if err != nil {
			log.Warnf("Error reading %s backup file #1. Err: '%s'", bkpType, err)
		} else {
			return bytes
		}
	}

	jsonFile, err = os.Open(backupName2)
	if err != nil {
		log.Errorf("Error opening %s backup file #2. Setting empty backup as default. Err: '%s'", bkpType, err)
		return nil
	} else {
		bytes, err := ioutil.ReadAll(jsonFile)
		if err != nil {
			log.Errorf("Error reading %s backup file #2. Setting empty backup as default. Err: '%s'", bkpType, err)
			return nil
		} else {
			return bytes
		}
	}
}

func LoadBackupedSignals() (map[int]int, map[int]int, map[string]int) {
	startingSignals := make(map[int]int)
	bkpStartSignals := LoadBackup(StartBkp)

	if bkpStartSignals != nil {
		json.Unmarshal([]byte(bkpStartSignals), &startingSignals)
		log.Infof("Starting signals restored from backup file. Signals: %v", startingSignals)
	}

	finishingSignals := make(map[int]int)
	bkpFinishSignals := LoadBackup(FinishBkp)

	if bkpFinishSignals != nil {
		json.Unmarshal([]byte(bkpFinishSignals), &finishingSignals)
		log.Infof("Finishing signals restored from backup file. Signals: %v", finishingSignals)
	}

	closingSignals := make(map[string]int)
	bkpCloseSignals := LoadBackup(CloseBkp)

	if bkpCloseSignals != nil {
		json.Unmarshal([]byte(bkpCloseSignals), &closingSignals)
		log.Infof("Closing signals restored from backup file. Signals: %v", closingSignals)
	}

	return startingSignals, finishingSignals, closingSignals
}

func StoreBackup(bkpType BackupType, data []byte) {
	path := calculateBackupPath(bkpType)
	writeBackup(fmt.Sprintf("%s/bkp.1", path), data)
	writeBackup(fmt.Sprintf("%s/bkp.2", path), data)
}

func writeBackup(backupFileName string, data []byte) {
	var backupFile *os.File

	_, err := os.Stat(backupFileName)
	if os.IsNotExist(err) {
		log.Infof("Creating backup file '%s'.", backupFileName)
		backupFile, err = os.Create(backupFileName)
		if err != nil {
			log.Fatalf("Error creating backup file '%s'. Err: %s", backupFileName, err)
		}
	} else {
		backupFile, err = os.OpenFile(backupFileName, os.O_RDWR, 0644)
    	if err != nil {
			log.Errorf("Error writing backup file '%s'. Err: %s", backupFileName, err)
			return
		}
	}

	backupFile.Write(data)
	backupFile.Close()
	log.Tracef("Backup file %s saved.", backupFileName)
}

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
