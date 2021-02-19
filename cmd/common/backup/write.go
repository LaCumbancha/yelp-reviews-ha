package backup

import (
	"os"
	"fmt"
	"encoding/json"

	log "github.com/sirupsen/logrus"
)

func StoreMultiFlowDataBackup(flow int, data string) {
	multiFlowData := setFlowAndData(MultiFlowBackup { Flow: flow, Data: data })
	path := calculateBackupPath(DataBkp)
	storeDataBackup(FileKey1, path, multiFlowData)
	storeDataBackup(FileKey2, path, multiFlowData)
}

func StoreSingleFlowDataBackup(data string) {
	path := calculateBackupPath(DataBkp)
	storeDataBackup(FileKey1, path, data)
	storeDataBackup(FileKey2, path, data)
}

func StoreSignalsBackup(data interface{}, bkpType BackupType) {
	bytes, err := json.Marshal(data)
	if err != nil {
		log.Errorf("Error serializing %s to backup. Err: %s", bkpType, err)
	} else {
		path := calculateBackupPath(bkpType)
		storeSpecialBackup(FileKey1, path, bytes)
		storeSpecialBackup(FileKey2, path, bytes)
	}
}

func storeDataBackup(fileKey string, path string, data string) {
	removeOk(fileKey, path)
	appendBackup(fileKey, path, data)
	writeOk(fileKey, path)
}

func storeSpecialBackup(fileKey string, path string, data []byte) {
	removeOk(fileKey, path)
	writeBackup(fileKey, path, data)
	writeOk(fileKey, path)
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

func writeOk(okFileKey string, path string) {
	okFileName := fmt.Sprintf("%s/ok.%s", path, okFileKey)
	okFile, err := os.Create(okFileName)
	log.Tracef("Creating ok file '%s'.", okFileName)
	if err != nil {
		log.Errorf("Error creating ok file '%s'. Err: %s", okFileName, err)
	}
	okFile.Close()
	log.Debugf("Ok file %s saved.", okFileName)
}

func writeBackup(backupFileKey string, path string, data []byte) {
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
			log.Errorf("Error opening backup file '%s'. Err: %s", backupFileName, err)
			return
		}
	}

	backupFile.Write(data)
	backupFile.Close()

	log.Debugf("Backup file %s saved.", backupFileName)
}

func appendBackup(backupFileKey string, path string, data string) {
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
		backupFile, err = os.OpenFile(backupFileName, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Errorf("Error opening backup file '%s'. Err: %s", backupFileName, err)
			return
		}
	}

	_, err = backupFile.WriteString(data + "\n")
	if err != nil {
		log.Errorf("Error writing backup file '%s'. Err: %s", backupFileName, err)
	}
	backupFile.Close()

	log.Debugf("Backup file %s updated.", backupFileName)
}