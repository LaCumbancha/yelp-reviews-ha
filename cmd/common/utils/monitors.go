package utils

import (
	"fmt"
)

func MonitorName(monitorIdx string) string {
	return fmt.Sprintf("monitor%s", monitorIdx)
}
