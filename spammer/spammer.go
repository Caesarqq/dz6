package main

import (
	"fmt"
	"sort"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	if len(cmds) == 0 {
		return
	}
	channels := make([]chan interface{}, len(cmds)+1)
	for i := 0; i < len(channels); i++ {
		channels[i] = make(chan interface{})
	}
	wg := &sync.WaitGroup{}

	for i := 0; i < len(cmds); i++ {
		wg.Add(1)
		currentCmd := cmds[i]
		inputChan := channels[i]
		outputChan := channels[i+1]

		go func(command cmd, in chan interface{}, out chan interface{}) {
			defer wg.Done()
			command(in, out)
			close(out)
		}(currentCmd, inputChan, outputChan)
	}
	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	seenUsers := make(map[uint64]bool)
	var mutex sync.Mutex
	var wg sync.WaitGroup
	maxWorkers := 100
	semaphore := make(chan struct{}, maxWorkers)

	for item := range in {
		email, ok := item.(string)
		if !ok {
			continue
		}
		wg.Add(1)
		semaphore <- struct{}{}

		go func(emailAddr string) {
			defer wg.Done()
			defer func() { <-semaphore }()
			user := GetUser(emailAddr)
			mutex.Lock()
			alreadySeen := seenUsers[user.ID]
			if !alreadySeen {
				seenUsers[user.ID] = true
				mutex.Unlock()
				out <- user
			} else {
				mutex.Unlock()
			}
		}(email)
	}
	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	var wg sync.WaitGroup
	var mutex sync.Mutex
	userBatch := make([]User, 0)
	sendBatch := func(batch []User) {
		if len(batch) == 0 {
			return
		}
		wg.Add(1)
		batchCopy := make([]User, len(batch))
		copy(batchCopy, batch)

		go func(users []User) {
			defer wg.Done()
			msgs, err := GetMessages(users...)
			if err != nil {
				return
			}
			for i := 0; i < len(msgs); i++ {
				out <- msgs[i]
			}
		}(batchCopy)
	}

	for item := range in {
		user, ok := item.(User)
		if !ok {
			continue
		}
		mutex.Lock()
		userBatch = append(userBatch, user)
		if len(userBatch) >= GetMessagesMaxUsersBatch {
			sendBatch(userBatch)
			userBatch = make([]User, 0)
		}
		mutex.Unlock()
	}
	mutex.Lock()
	if len(userBatch) > 0 {
		sendBatch(userBatch)
	}
	mutex.Unlock()
	wg.Wait()
}

func CheckSpam(in, out chan interface{}) {
	limiter := make(chan struct{}, HasSpamMaxAsyncRequests)
	var wg sync.WaitGroup

	for item := range in {
		msgID, ok := item.(MsgID)
		if !ok {
			continue
		}
		wg.Add(1)
		limiter <- struct{}{}

		go func(id MsgID) {
			defer wg.Done()
			defer func() {
				<-limiter
			}()
			isSpam, err := HasSpam(id)
			if err != nil {
				isSpam = false
			}
			result := MsgData{
				ID:      id,
				HasSpam: isSpam,
			}
			out <- result
		}(msgID)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	allResults := make([]MsgData, 0)

	for item := range in {
		msgData, ok := item.(MsgData)
		if !ok {
			continue
		}
		allResults = append(allResults, msgData)
	}

	sort.Slice(allResults, func(i, j int) bool {
		first := allResults[i]
		second := allResults[j]

		if first.HasSpam != second.HasSpam {
			if first.HasSpam {
				return true
			}
			return false
		}
		if first.ID < second.ID {
			return true
		}
		return false
	})

	for i := 0; i < len(allResults); i++ {
		msg := allResults[i]
		outputStr := fmt.Sprintf("%v %d", msg.HasSpam, msg.ID)
		out <- outputStr
	}
}
