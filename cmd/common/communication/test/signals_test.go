package test

import (
	"testing"
	comms "github.com/LaCumbancha/yelp-review-ha/cmd/common/communication"
)

func TestSignalsControl(test *testing.T) {
	multiDatasetMap := make(map[int]map[string]map[string]bool)
	signalsNeeded := map[string]int{"INPUT-0": 2, "INPUT-1": 1}
	savedInputs := []string{"INPUT-1"}
	
	veryFirst, first, new, all, total := comms.MultiDatasetSignalsControl(0, "INPUT-0", "0", multiDatasetMap, signalsNeeded, savedInputs)
	if !veryFirst { test.Errorf("VeryFirst should be true.") }
	if !first { test.Errorf("First should be true.") }
	if !new { test.Errorf("New should be true.") }
	if all { test.Errorf("All should be false.") }
	if total { test.Errorf("Total should be false.") }

	veryFirst, first, new, all, total = comms.MultiDatasetSignalsControl(0, "INPUT-0", "0", multiDatasetMap, signalsNeeded, savedInputs)
	if veryFirst { test.Errorf("VeryFirst should be false.") }
	if first { test.Errorf("First should be false.") }
	if new { test.Errorf("New should be false.") }
	if all { test.Errorf("All should be false.") }
	if total { test.Errorf("Total should be false.") }
	
	veryFirst, first, new, all, total = comms.MultiDatasetSignalsControl(0, "INPUT-1", "0", multiDatasetMap, signalsNeeded, savedInputs)
	if veryFirst { test.Errorf("VeryFirst should be false.") }
	if !first { test.Errorf("First should be true.") }
	if !new { test.Errorf("New should be true.") }
	if !all { test.Errorf("All should be true.") }
	if total { test.Errorf("Total should be false.") }
	
	veryFirst, first, new, all, total = comms.MultiDatasetSignalsControl(0, "INPUT-0", "1", multiDatasetMap, signalsNeeded, savedInputs)
	if veryFirst { test.Errorf("VeryFirst should be false.") }
	if first { test.Errorf("First should be false.") }
	if !new { test.Errorf("New should be true.") }
	if !all { test.Errorf("All should be true.") }
	if !total { test.Errorf("Total should be true.") }

	veryFirst, first, new, all, total = comms.MultiDatasetSignalsControl(1, "INPUT-0", "0", multiDatasetMap, signalsNeeded, savedInputs)
	if !veryFirst { test.Errorf("VeryFirst should be true.") }
	if !first { test.Errorf("First should be true.") }
	if !new { test.Errorf("New should be true.") }
	if all { test.Errorf("All should be false.") }
	if total { test.Errorf("Total should be false.") }
	
	veryFirst, first, new, all, total = comms.MultiDatasetSignalsControl(1, "INPUT-0", "0", multiDatasetMap, signalsNeeded, savedInputs)
	if veryFirst { test.Errorf("VeryFirst should be false.") }
	if first { test.Errorf("First should be false.") }
	if new { test.Errorf("New should be false.") }
	if all { test.Errorf("All should be false.") }
	if total { test.Errorf("Total should be false.") }
	
	veryFirst, first, new, all, total = comms.MultiDatasetSignalsControl(1, "INPUT-1", "0", multiDatasetMap, signalsNeeded, savedInputs)
	if veryFirst { test.Errorf("VeryFirst should be false.") }
	if !first { test.Errorf("First should be true.") }
	if !new { test.Errorf("New should be true.") }
	if !all { test.Errorf("All should be true.") }
	if total { test.Errorf("Total should be false.") }
	
	veryFirst, first, new, all, total = comms.MultiDatasetSignalsControl(1, "INPUT-0", "1", multiDatasetMap, signalsNeeded, savedInputs)
	if veryFirst { test.Errorf("VeryFirst should be false.") }
	if first { test.Errorf("First should be false.") }
	if !new { test.Errorf("New should be true.") }
	if !all { test.Errorf("All should be true.") }
	if !total { test.Errorf("Total should be true.") }
	
	veryFirst, first, new, all, total = comms.MultiDatasetSignalsControl(1, "INPUT-1", "1", multiDatasetMap, signalsNeeded, savedInputs)
	if veryFirst { test.Errorf("VeryFirst should be false.") }
	if first { test.Errorf("First should be false.") }
	if !new { test.Errorf("New should be false.") }
	if all { test.Errorf("All should be false.") }
	if !total { test.Errorf("Total should be false.") }
	
	veryFirst, first, new, all, total = comms.MultiDatasetSignalsControl(2, "INPUT-1", "0", multiDatasetMap, signalsNeeded, savedInputs)
	if !veryFirst { test.Errorf("VeryFirst should be true.") }
	if !first { test.Errorf("First should be true.") }
	if !new { test.Errorf("New should be true.") }
	if !all { test.Errorf("All should be true.") }
	if total { test.Errorf("Total should be false.") }
	
	veryFirst, first, new, all, total = comms.MultiDatasetSignalsControl(2, "INPUT-0", "0", multiDatasetMap, signalsNeeded, savedInputs)
	if veryFirst { test.Errorf("VeryFirst should be false.") }
	if !first { test.Errorf("First should be true.") }
	if !new { test.Errorf("New should be true.") }
	if all { test.Errorf("All should be false.") }
	if total { test.Errorf("Total should be false.") }
	
	veryFirst, first, new, all, total = comms.MultiDatasetSignalsControl(2, "INPUT-0", "1", multiDatasetMap, signalsNeeded, savedInputs)
	if veryFirst { test.Errorf("VeryFirst should be false.") }
	if first { test.Errorf("First should be false.") }
	if !new { test.Errorf("New should be true.") }
	if !all { test.Errorf("All should be true.") }
	if !total { test.Errorf("Total should be true.") }
	
	veryFirst, first, new, all, total = comms.MultiDatasetSignalsControl(3, "INPUT-0", "0", multiDatasetMap, signalsNeeded, savedInputs)
	if !veryFirst { test.Errorf("VeryFirst should be true.") }
	if !first { test.Errorf("First should be true.") }
	if !new { test.Errorf("New should be true.") }
	if all { test.Errorf("All should be false.") }
	if total { test.Errorf("Total should be false.") }
	
	veryFirst, first, new, all, total = comms.MultiDatasetSignalsControl(3, "INPUT-0", "1", multiDatasetMap, signalsNeeded, savedInputs)
	if veryFirst { test.Errorf("VeryFirst should be false.") }
	if first { test.Errorf("First should be false.") }
	if !new { test.Errorf("New should be true.") }
	if !all { test.Errorf("All should be true.") }
	if !total { test.Errorf("Total should be true.") }
	
	singleDatasetMap := make(map[string]map[string]bool)
	signalsNeeded = map[string]int{"INPUT-0": 2}

	veryFirst, first, new, all, total = comms.SingleDatasetSignalsControl("INPUT-0", "0", singleDatasetMap, signalsNeeded)
	if !veryFirst { test.Errorf("VeryFirst should be true.") }
	if !first { test.Errorf("First should be true.") }
	if !new { test.Errorf("New should be true.") }
	if all { test.Errorf("All should be false.") }
	if total { test.Errorf("Total should be false.") }
	
	veryFirst, first, new, all, total = comms.SingleDatasetSignalsControl("INPUT-0", "0", singleDatasetMap, signalsNeeded)
	if veryFirst { test.Errorf("VeryFirst should be false.") }
	if first { test.Errorf("First should be false.") }
	if new { test.Errorf("New should be false.") }
	if all { test.Errorf("All should be false.") }
	if total { test.Errorf("Total should be false.") }
	
	veryFirst, first, new, all, total = comms.SingleDatasetSignalsControl("INPUT-0", "1", singleDatasetMap, signalsNeeded)
	if veryFirst { test.Errorf("VeryFirst should be false.") }
	if first { test.Errorf("First should be false.") }
	if !new { test.Errorf("New should be true.") }
	if !all { test.Errorf("All should be true.") }
	if !total { test.Errorf("Total should be true.") }
	
	veryFirst, first, new, all, total = comms.SingleDatasetSignalsControl("INPUT-0", "1", singleDatasetMap, signalsNeeded)
	if veryFirst { test.Errorf("VeryFirst should be false.") }
	if first { test.Errorf("First should be false.") }
	if new { test.Errorf("New should be false.") }
	if all { test.Errorf("All should be false.") }
	if total { test.Errorf("Total should be false.") }
}
