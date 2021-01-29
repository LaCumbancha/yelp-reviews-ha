package core

import (
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

func GenerateOutputSignals(funbizSigs int, weekdaysSigs int, hashesSigs int, usersSigs int, starsSigs int) int {
	maxSignals := 0

	funbizSignals := comms.EndSignals(funbizSigs)
	weekdaySignals := comms.EndSignals(weekdaysSigs)
	hashesSignals := comms.EndSignals(hashesSigs)
	usersSignals := comms.EndSignals(usersSigs)
	starsSignals := comms.EndSignals(starsSigs)

	if funbizSignals > maxSignals {
		maxSignals = funbizSignals
	}

	if weekdaySignals > maxSignals {
		maxSignals = weekdaySignals
	}

	if hashesSignals > maxSignals {
		maxSignals = hashesSignals
	}

	if usersSignals > maxSignals {
		maxSignals = usersSignals
	}

	if starsSignals > maxSignals {
		maxSignals = starsSignals
	}

	return maxSignals
}
