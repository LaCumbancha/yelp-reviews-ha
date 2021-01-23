package core

import (
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

func GenerateSignalsMap(funbizSigs int, weekdaysSigs int, hashesSigs int, usersSigs int, starsSigs int) map[string]int {
	signalsMap := make(map[string]int)

	signalsMap[Funbiz] = comms.EndSignals(funbizSigs)
	signalsMap[Weekdays] = comms.EndSignals(weekdaysSigs)
	signalsMap[Hashes] = comms.EndSignals(hashesSigs)
	signalsMap[Users] = comms.EndSignals(usersSigs)
	signalsMap[Stars] = comms.EndSignals(starsSigs)

	return signalsMap
}
