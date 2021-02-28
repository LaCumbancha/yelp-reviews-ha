package utils

import (
	"fmt"
	"strings"
)

func NetworkErrorText(err error) string {
	errTxt := fmt.Sprintf("%s", err)

	if strings.HasSuffix(err.Error(), "no such host") {
		errTxt = "Couldn't find host."
	}

	if strings.HasSuffix(err.Error(), "connection refused") {
		errTxt = "Connection refused."
	}

	return errTxt
}
