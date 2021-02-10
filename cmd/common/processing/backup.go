package processing

import (
	"os"
	"fmt"
	"io/ioutil"

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

	_, err1 := os.Stat(fmt.Sprintf("%s/bkp.1", path))
	_, err2 := os.Stat(fmt.Sprintf("%s/bkp.2", path))

	if os.IsNotExist(err1) && os.IsNotExist(err2) {
		log.Warnf("No %s backup file found. Setting empty backup as default.", bkpType)
		return nil
	}

	jsonFile, err := os.Open(fmt.Sprintf("%s/bkp.1", path))
	if err != nil {
		log.Warnf("Error opening %s backup file #1. Err: '%s'", bkpType, err)
		jsonFile, err = os.Open(fmt.Sprintf("%s/bkp.2", path))
		if err != nil {
			log.Fatalf("Couldn't open %s backup file. Err: '%s'", bkpType, err)
		}
	}

	bytes, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		log.Errorf("Error reading %s backup file. Setting empty backup as default. Err: '%s'", bkpType, err)
		return nil
	} else {
		return bytes
	}
}

func StoreBackup(bkpType BackupType, data []byte) {
	path := calculateBackupPath(bkpType)
	writeBackup(fmt.Sprintf("%s/bkp.1", path), data)
	writeBackup(fmt.Sprintf("%s/bkp.2", path), data)
}

func writeBackup(name string, data []byte) {
	log.Tracef("Creating backup file '%s'.", name)
	bkpFile, err := os.Create(name)
	if err != nil {
		log.Fatalf("Error creating backup file '%s'. Err: %s", name, err)
	} else {
		bkpFile.Write(data)
		bkpFile.Close()
		log.Tracef("Backup file %s saved.", name)
	}
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
