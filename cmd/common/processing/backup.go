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

func InitializeBackupStructure() {
	if _, err := os.Stat(BkpMainPath); os.IsNotExist(err) {
		path := BkpMainPath
		log.Infof("Creating backup structures from scratch.")

		err := os.Mkdir(path, os.ModePerm)
		if err != nil {
			log.Fatalf("Error creating main backup folder. Err: '%s'", err)
		}

		path = fmt.Sprintf("%s/%s", BkpMainPath, StartPath)
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			log.Fatalf("Error creating starting signals backup folder. Err: '%s'", err)
		} else {
			initializeBackupFiles(path)
		}
	
		path = fmt.Sprintf("%s/%s", BkpMainPath, FinishPath)
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			log.Fatalf("Error creating finishing signals backup folder. Err: '%s'", err)
		} else {
			initializeBackupFiles(path)
		}
	
		path = fmt.Sprintf("%s/%s", BkpMainPath, ClosePath)
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			log.Fatalf("Error creating closing signals backup folder. Err: '%s'", err)
		} else {
			initializeBackupFiles(path)
		}
	
		path = fmt.Sprintf("%s/%s", BkpMainPath, ReceivedPath)
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			log.Fatalf("Error creating received messages backup folder. Err: '%s'", err)
		} else {
			initializeBackupFiles(path)
		}
	
		path = fmt.Sprintf("%s/%s", BkpMainPath, DataPath)
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			log.Fatalf("Error creating data backup folder. Err: '%s'", err)
		} else {
			initializeBackupFiles(path)
		}
	} else {
		log.Infof("Backup directory found.")
	}
}

func initializeBackupFiles(path string) {
	emptyContent := []byte("")
	writeBackup(FileKey1, path, emptyContent)
	writeBackup(FileKey2, path, emptyContent)
}

func LoadBackupedSignals() (map[int]int, map[int]int, map[string]int, map[string]bool) {
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

	receivedMessages := make(map[string]bool)
	bkpReceivedMessages := LoadBackup(ReceivedBkp)

	if bkpReceivedMessages != nil {
		json.Unmarshal([]byte(bkpReceivedMessages), &receivedMessages)
		log.Infof("Received messages restored from backup file. Messages: %d", len(receivedMessages))
	}

	return startingSignals, finishingSignals, closingSignals, receivedMessages
}

func LoadBackup(bkpType BackupType) []byte {
	path := calculateBackupPath(bkpType)

	bkpBytes := loadBackupFromFile(FileKey1, path)
	if bkpBytes == nil || string(bkpBytes) == "" {
		if bkpBytes == nil {
			log.Warnf("Couldn't load '%s' backup file #%s. Attempting with #%s.", bkpType, FileKey1, FileKey2)
		} else {
			log.Tracef("Empty '%s' backup file #%s ignored. Attempting with #%s.", bkpType, FileKey1, FileKey2)
			bkpBytes = nil
		}
		
		bkpBytes = loadBackupFromFile(FileKey2, path)
		if bkpBytes == nil || string(bkpBytes) == "" {
			if bkpBytes == nil {
				log.Errorf("Couldn't load '%s' backup file #%s. Setting empty backup as default.", bkpType, FileKey2)
			} else {
				log.Tracef("Empty '%s' backup file #%s ignored. Setting empty backup as default.", bkpType, FileKey2)
				bkpBytes = nil
			}
		}
	}

	return bkpBytes
}

func loadBackupFromFile(fileKey string, path string) []byte {
	okFileName := fmt.Sprintf("%s/ok.%s", path, fileKey)
	backupFileName := fmt.Sprintf("%s/bkp.%s", path, fileKey)

	_, err := os.Stat(okFileName)
	if os.IsNotExist(err) {
		log.Warnf("Ok file #%s not found.", fileKey)
	} else {
		jsonFile, err := os.Open(backupFileName)
		if err != nil {
			log.Warnf("Error opening backup file #%s. Err: '%s'", fileKey, err)
		} else {
			bytes, err := ioutil.ReadAll(jsonFile)
			if err != nil {
				log.Warnf("Error reading backup file #%s. Err: '%s'", fileKey, err)
			} else {
				return bytes
			}
		}
	}

	return nil
}

func StoreBackup(data interface{}, bkpType BackupType) {
	bytes, err := json.Marshal(data)
	if err != nil {
		log.Errorf("Error serializing %s. Err: %s", bkpType, err)
	} else {
		path := calculateBackupPath(bkpType)
		removeOk(FileKey1, path)
		writeBackup(FileKey1, path, bytes)
		removeOk(FileKey2, path)
		writeBackup(FileKey2, path, bytes)
	}
}

func removeOk(okFileKey string, path string) {
	okFileName := fmt.Sprintf("%s/ok.%s", path, okFileKey)
	_, err := os.Stat(okFileName)
	if os.IsNotExist(err) {
		log.Warnf("Ok file '%s' not found.", okFileName)
	} else {
		err = os.Remove(okFileName)
		if err != nil {
		  log.Errorf("Error removing ok file '%s'. Err: %s", okFileName, err)
		}
	}
}

func writeBackup(backupFileKey string, path string, data []byte) {
	okFileName := fmt.Sprintf("%s/ok.%s", path, backupFileKey)
	backupFileName := fmt.Sprintf("%s/bkp.%s", path, backupFileKey)
	var backupFile *os.File

	_, err := os.Stat(backupFileName)
	if os.IsNotExist(err) {
		log.Debugf("Creating backup file '%s'.", backupFileName)
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

	okFile, err := os.Create(okFileName)
	log.Tracef("Creating ok file '%s'.", okFileName)
	if err != nil {
		log.Errorf("Error creating ok file '%s'. Err: %s", okFileName, err)
	}
	okFile.Close()

	log.Debugf("Backup file %s saved.", backupFileName)
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
	case ReceivedBkp:
		path = BkpMainPath + "/" + ReceivedPath
	case DataBkp:
		path = BkpMainPath + "/" + DataPath
	default:
		log.Fatalf("Unexpected backup type: %s.", bkpType)
	}

	return path
}
