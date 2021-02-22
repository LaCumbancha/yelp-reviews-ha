package backup

import (
	"os"
	"fmt"

	log "github.com/sirupsen/logrus"
)

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
		}
	} else {
		log.Infof("Backup directory found.")
	}
}

func InitializeDatasetBackup(dataset int) {
	mainPath := calculateBackupPath(DataBkp)
	datasetBackupPath := fmt.Sprintf("%s/dataset.%d", mainPath, dataset)

	err := os.MkdirAll(datasetBackupPath, os.ModePerm)
	if err != nil {
		log.Errorf("Error creating dataset #%d backup folder. Err: '%s'", dataset, err)
	} else {
		initializeBackupFiles(datasetBackupPath)
	}
}

func RemoveDatasetBackup(dataset int) {
	mainPath := calculateBackupPath(DataBkp)
	datasetPath := fmt.Sprintf("%s/dataset.%d", mainPath, dataset)

	err := os.RemoveAll(datasetPath)
	if err != nil {
		log.Errorf("Error removing dataset #%d backup. Err: %s", dataset, err)
	}
}

func initializeBackupFiles(path string) {
	emptyContent := []byte("")
	writeBackup(FileKey1, path, emptyContent)
	writeOk(FileKey1, path)
	writeBackup(FileKey2, path, emptyContent)
	writeOk(FileKey2, path)
}
