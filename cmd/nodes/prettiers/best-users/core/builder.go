package core

import (
	"fmt"
	"sync"
	"sort"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type Builder struct {
	data 			map[string]int
	mutex 			*sync.Mutex
	reviews			int
}

func NewBuilder(minReviews int) *Builder {
	builder := &Builder {
		data:		make(map[string]int),
		mutex:		&sync.Mutex{},
		reviews:	minReviews,
	}

	return builder
}

func (builder *Builder) Save(rawData string) {
	var userData comms.UserData
	json.Unmarshal([]byte(rawData), &userData)

	builder.mutex.Lock()

	if oldReviews, found := builder.data[userData.UserId]; found {
	    log.Warnf("User %s was already stored with %d 5-stars reviews (new value: %d).", userData.UserId, oldReviews, userData.Reviews)
	} else {
		builder.data[userData.UserId] = userData.Reviews
	}

	builder.mutex.Unlock()
}

func (builder *Builder) BuildData() string {
	var usersList []comms.UserData
	for userId, reviews := range builder.data {
		user := comms.UserData { UserId: userId, Reviews: reviews }
		usersList = append(usersList, user)
    }

    sort.SliceStable(usersList, func(userIdx1, userIdx2 int) bool {
	    return usersList[userIdx1].Reviews > usersList[userIdx2].Reviews
	})

	response := fmt.Sprintf("Users with +%d reviews (only 5-stars): ", builder.reviews)

	for _, user := range usersList {
		response += fmt.Sprintf("%s (%d) ; ", user.UserId, user.Reviews)
    }

    if len(usersList) == 0 {
    	return response + "no users accomplish that requirements."
    } else {
    	return response[0:len(response)-3]
    }
}
