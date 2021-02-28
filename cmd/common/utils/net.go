package utils

import (
	"strings"
)

func IsNoSuchHost(err error) bool {
	return strings.HasSuffix(err.Error(), "no such host")
}

func IsConnectionRefused(err error) bool {
	return strings.HasSuffix(err.Error(), "connection refused")
}
