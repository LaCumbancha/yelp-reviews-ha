package logger

import (
	"sync"
	log "github.com/sirupsen/logrus"
)

type Logger struct {
	bulkRate int
}

var logger *Logger
var once sync.Once

func Instance() *Logger {
	once.Do(func() { logger = &Logger{ bulkRate: 1 } })
	return logger
}

func (logger *Logger) SetBulkRate(bulkRate int) {
	logger.bulkRate = bulkRate
}

func (logger *Logger) Infof(message string, bulk int) {
	if (bulk % logger.bulkRate == 0) {
		log.Infof(message)
	}
}

func (logger *Logger) Debugf(message string, bulk int) {
	if (bulk % logger.bulkRate == 0) {
		log.Debugf(message)
	}
}
