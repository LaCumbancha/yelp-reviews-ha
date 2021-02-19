package processing

import (
	"strconv"
)

// Defining message storage code
func MessageSavingId(nodeCode string, dataset int, bulk int) string {
	return nodeCode + "." + strconv.Itoa(dataset) + "." + strconv.Itoa(bulk)
}
