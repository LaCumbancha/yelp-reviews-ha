package utils

import (
	"fmt"
	"bytes"
    "os/exec"
    log "github.com/sirupsen/logrus"
)

func MonitorName(monitorIdx string) string {
	return fmt.Sprintf("monitor%s", monitorIdx)
}

func MonitorIP(monitorName string) string {
	cmd := exec.Command("./scripts/monitor-ip", monitorName)

    var out bytes.Buffer
    cmd.Stdout = &out

    if err := cmd.Run(); err != nil {
        log.Errorf("Error executing IP-Finder script. Err: %s", err)
    }

    ip := out.String()

    if ip[len(ip)-1:] == "\n" {
		return ip[:len(ip)-1]
	} else {
		return ip
	}
}