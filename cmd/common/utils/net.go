package utils

import (
	"strings"
)

func IsNoSuchHost(err error) bool {
	return strings.HasSuffix(err.Error(), "no such host")
}
