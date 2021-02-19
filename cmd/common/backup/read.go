package backup

import (
	"os"
	"fmt"
	"strings"
	"io/ioutil"
	"encoding/json"

	log "github.com/sirupsen/logrus"
)

func LoadDataBackup() []DataBackup {
	data := make([]DataBackup, 0)
	backup := loadBackup(DataBkp)

	if backup != nil {
		backupRawData := string(backup)
		backupDataList := strings.Split(backupRawData, "\n")
		for _, backupData := range backupDataList {
			if backupData != "" {
				backupDataUnpackaged := unpackageBackupMessage(backupData)
				if backupDataUnpackaged.Dataset != -1 {
					data = append(data, backupDataUnpackaged)
				} else {
					log.Warnf("Error parsing backup information. Data: '%s'", backupDataUnpackaged)
				}
			}
		}
	}

	return data
}

func LoadSignalsBackup() (map[int]int, map[int]int, map[string]int, map[string]bool) {
	startingSignals := make(map[int]int)
	bkpStartSignals := loadBackup(StartBkp)

	if bkpStartSignals != nil {
		json.Unmarshal([]byte(bkpStartSignals), &startingSignals)
		log.Infof("Starting signals restored from backup file. Signals: %v", startingSignals)
	}

	finishingSignals := make(map[int]int)
	bkpFinishSignals := loadBackup(FinishBkp)

	if bkpFinishSignals != nil {
		json.Unmarshal([]byte(bkpFinishSignals), &finishingSignals)
		log.Infof("Finishing signals restored from backup file. Signals: %v", finishingSignals)
	}

	closingSignals := make(map[string]int)
	bkpCloseSignals := loadBackup(CloseBkp)

	if bkpCloseSignals != nil {
		json.Unmarshal([]byte(bkpCloseSignals), &closingSignals)
		log.Infof("Closing signals restored from backup file. Signals: %v", closingSignals)
	}

	receivedMessages := make(map[string]bool)
	bkpReceivedMessages := loadBackup(ReceivedBkp)

	if bkpReceivedMessages != nil {
		json.Unmarshal([]byte(bkpReceivedMessages), &receivedMessages)
		log.Infof("Received messages restored from backup file. Messages: %d", len(receivedMessages))
	}

	return startingSignals, finishingSignals, closingSignals, receivedMessages
}

func loadBackup(bkpType BackupType) []byte {
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
