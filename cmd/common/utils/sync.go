package utils

import (
	"sync"
)

func DefineWaitGroupByDataset(dataset int, datasetsWgs map[int]*sync.WaitGroup, datasetsWgMutex *sync.Mutex) *sync.WaitGroup {
	datasetsWgMutex.Lock()
	if _, found := datasetsWgs[dataset]; !found {
		datasetsWgs[dataset] = &sync.WaitGroup{}
	}
	datasetWg := datasetsWgs[dataset]
	datasetsWgMutex.Unlock()
	return datasetWg
}

func WaitGroupByDataset(dataset int, datasetsWgs map[int]*sync.WaitGroup, datasetsWgMutex *sync.Mutex) *sync.WaitGroup {
	datasetsWgMutex.Lock()
	datasetWg := datasetsWgs[dataset]
	datasetsWgMutex.Unlock()
	return datasetWg
}

func DeleteWaitGroupByDataset(dataset int, datasetsWgs map[int]*sync.WaitGroup, datasetsWgMutex *sync.Mutex) {
	datasetsWgMutex.Lock()
	if _, found := datasetsWgs[dataset]; found {
		delete(datasetsWgs, dataset);
	}
	datasetsWgMutex.Unlock()
}

func AllDatasetsWaitGroups(datasetsWgs map[int]*sync.WaitGroup, datasetsWgMutex *sync.Mutex) []*sync.WaitGroup {
	var waitGroups []*sync.WaitGroup

	datasetsWgMutex.Lock()
	for _, datasetWg := range datasetsWgs {
		waitGroups = append(waitGroups, datasetWg)
	}
	datasetsWgMutex.Unlock()

	return waitGroups
}
