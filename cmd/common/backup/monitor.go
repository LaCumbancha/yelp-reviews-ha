package backup

import (
	"os"
	log "github.com/sirupsen/logrus"
)

func InitializeMonitorBackup(state string) {
	if _, err := os.Stat(BkpMainPath); os.IsNotExist(err) {
		log.Infof("Creating backup structures from scratch.")
		if err := os.Mkdir(BkpMainPath, os.ModePerm); err != nil {
			log.Fatalf("Error creating main backup folder. Err: '%s'", err)
		} else {
			initializeMonitorBackupState(state)
		}
	} else {
		log.Infof("Backup directory found.")
	}
}

func initializeMonitorBackupState(state string) {
	stateBytes := []byte(state)
	writeBackup(FileKey1, BkpMainPath, stateBytes)
	writeOk(FileKey1, BkpMainPath)
	writeBackup(FileKey2, BkpMainPath, stateBytes)
	writeOk(FileKey2, BkpMainPath)
}

func UpdateMonitorBackupState(state string) {
	stateBytes := []byte(state)
	storeSimpleBackup(FileKey1, BkpMainPath, stateBytes)
	storeSimpleBackup(FileKey2, BkpMainPath, stateBytes)
}

func LoadMonitorBackupState() string {
	return string(loadBackup(BkpMainPath))
}
