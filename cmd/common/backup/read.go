package backup

import (
	"os"
	"fmt"
	"strings"
	"io/ioutil"
	"encoding/json"

	log "github.com/sirupsen/logrus"
)

type InnerDataBackup struct {
	Dataset		int
	Data		[]byte
}

func LoadDataBackup() []DataBackup {
	data := make([]DataBackup, 0)
	backups := loadMultiBackup(DataBkp)

	for _, backup := range backups {
		backupRawData := string(backup.Data)
		backupDataList := strings.Split(backupRawData, "\n")
		for _, backupData := range backupDataList {
			if backupData != "" {
				backupDataUnpackaged := unpackBackupMessage(backupData)
				if backupDataUnpackaged.Flow != -1 {
					backupDataUnpackaged.Dataset = backup.Dataset
					data = append(data, backupDataUnpackaged)
				} else {
					log.Warnf("Error parsing backup information. Data: '%s'", string(backupDataUnpackaged.Data))
				}
			}
		}
	}

	return data
}

func LoadSignalsBackup() (map[int]map[string]map[string]bool, map[int]map[string]map[string]bool, map[string]map[string]bool, map[string]bool) {
	startingSignals := make(map[int]map[string]map[string]bool)
	bkpStartSignals := loadCommonBackup(StartBkp)

	if bkpStartSignals != nil {
		json.Unmarshal([]byte(bkpStartSignals), &startingSignals)
		log.Infof("Starting signals restored from backup file. Signals: %v", startingSignals)
	}

	finishingSignals := make(map[int]map[string]map[string]bool)
	bkpFinishSignals := loadCommonBackup(FinishBkp)

	if bkpFinishSignals != nil {
		json.Unmarshal([]byte(bkpFinishSignals), &finishingSignals)
		log.Infof("Finishing signals restored from backup file. Signals: %v", finishingSignals)
	}

	closingSignals := make(map[string]map[string]bool)
	bkpCloseSignals := loadCommonBackup(CloseBkp)

	if bkpCloseSignals != nil {
		json.Unmarshal([]byte(bkpCloseSignals), &closingSignals)
		log.Infof("Closing signals restored from backup file. Signals: %v", closingSignals)
	}

	receivedMessages := make(map[string]bool)
	bkpReceivedMessages := loadCommonBackup(ReceivedBkp)

	if bkpReceivedMessages != nil {
		json.Unmarshal([]byte(bkpReceivedMessages), &receivedMessages)
		log.Infof("Received messages restored from backup file. Messages: %d", len(receivedMessages))
	}

	return startingSignals, finishingSignals, closingSignals, receivedMessages
}

func loadCommonBackup(bkpType BackupType) []byte {
	path := calculateBackupPath(bkpType)
	return loadBackup(path)
}

func loadMultiBackup(bkpType BackupType) []InnerDataBackup {
	mainPath := calculateBackupPath(bkpType)

	backups := make([]InnerDataBackup, 0)
	backupFolders, err := ioutil.ReadDir(mainPath)
	if err != nil {
		log.Errorf("Couldn't open data backup folder. Err: %s", err)
	} else {
		log.Debugf("Data backup folders found: %d.", len(backupFolders))

		for _, backupFolder := range backupFolders {
			backupFolderName := backupFolder.Name()
			dataset := datasetFromBackupDirectory(backupFolderName)
			if dataset != -1 {
				backupPath := fmt.Sprintf("%s/%s", mainPath, backupFolderName)
				backups = append(backups, InnerDataBackup{ Dataset: dataset, Data: loadBackup(backupPath) })
			} else {
				log.Warnf("Error parsing backup folder dataset. Directory: '%s'", backupFolderName)
			}
		}
	}

	return backups
}

func loadBackup(path string) []byte {
	bkpBytes := loadBackupFromDirectory(FileKey1, path)
	if bkpBytes == nil || string(bkpBytes) == "" {
		if bkpBytes == nil {
			log.Warnf("Couldn't load backup file '%s' #%s. Attempting with #%s.", path, FileKey1, FileKey2)
		} else {
			log.Tracef("Empty backup file '%s' #%s ignored. Attempting with #%s.", path, FileKey1, FileKey2)
			bkpBytes = nil
		}
		
		bkpBytes = loadBackupFromDirectory(FileKey2, path)
		if bkpBytes == nil || string(bkpBytes) == "" {
			if bkpBytes == nil {
				log.Errorf("Couldn't load backup file '%s' #%s. Setting empty backup as default.", path, FileKey2)
			} else {
				log.Tracef("Empty backup file '%s' #%s ignored. Setting empty backup as default.", path, FileKey2)
				bkpBytes = nil
			}
		}
	}

	return bkpBytes
}

func loadBackupFromDirectory(fileKey string, path string) []byte {
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
