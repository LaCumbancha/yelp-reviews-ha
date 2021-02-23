package core

func GenerateOutputSignals(funbizSigs int, weekdaysSigs int, hashesSigs int, usersSigs int, starsSigs int) int {
	maxSignals := 0

	if funbizSigs > maxSignals {
		maxSignals = funbizSigs
	}

	if weekdaysSigs > maxSignals {
		maxSignals = weekdaysSigs
	}

	if hashesSigs > maxSignals {
		maxSignals = hashesSigs
	}

	if usersSigs > maxSignals {
		maxSignals = usersSigs
	}

	if starsSigs > maxSignals {
		maxSignals = starsSigs
	}

	return maxSignals
}
