package main

import (
	"fmt"
	"sort"
	"sync"
)

// конвейер: связываем cmd последовательно каналами и гарантируем свободное
// протекание данных без накопления между стадиями.[attached_file:3][attached_file:4]
func RunPipeline(cmds ...cmd) {
	if len(cmds) == 0 {
		return
	}

	var in chan interface{}

	for _, c := range cmds {
		out := make(chan interface{})

		// Для каждой стадии запускаем отдельную горутину, которая по завершении
		// закрывает свой выходной канал.[attached_file:3][attached_file:5]
		go func(c cmd, in, out chan interface{}) {
			defer close(out)
			c(in, out)
		}(c, in, out)

		in = out
	}

	// Дренируем последний канал, чтобы дождаться завершения всего конвейера.[attached_file:3][attached_file:4]
	for range in {
	}
}

// SelectUsers:
//
//	in  - string (email) внутри interface{}{}
//	out - User (уникальные пользователи по ID, с учётом алиасов).[attached_file:4][attached_file:5]
func SelectUsers(in, out chan interface{}) {

	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[uint64]struct{})

	for v := range in {
		email, ok := v.(string)
		if !ok {
			continue
		}

		wg.Add(1)
		go func(email string) {
			defer wg.Done()

			user := GetUser(email) // 1 секунда, можно параллельно.[attached_file:5]

			mu.Lock()
			if _, exists := seen[user.ID]; !exists {
				seen[user.ID] = struct{}{}
				mu.Unlock()
				// Отправляем только уникальных пользователей.[attached_file:3][attached_file:4]
				out <- user
			} else {
				mu.Unlock()
			}
		}(email)
	}

	// Ждём всех GetUser, чтобы после возврата не было записей в закрытый канал.[attached_file:3][attached_file:5]
	wg.Wait()
}

// SelectMessages:
//
//	in  - User
//	out - MsgID
//
// Использует батчи по 2 пользователя для оптимального числа вызовов GetMessages.[attached_file:4][attached_file:5]
func SelectMessages(in, out chan interface{}) {

	var wg sync.WaitGroup
	batch := make([]User, 0, GetMessagesMaxUsersBatch)

	// Отправка одного батча в GetMessages в отдельной горутине.[attached_file:5]
	runBatch := func(users []User) {
		if len(users) == 0 {
			return
		}
		wg.Add(1)
		go func(users []User) {
			defer wg.Done()
			msgs, err := GetMessages(users...)
			if err != nil {
				return
			}
			for _, id := range msgs {
				out <- id
			}
		}(users)
	}

	for v := range in {
		user, ok := v.(User)
		if !ok {
			continue
		}

		batch = append(batch, user)
		if len(batch) == GetMessagesMaxUsersBatch {
			// Копируем, чтобы не шарить слайс между батчами.[attached_file:5]
			b := make([]User, len(batch))
			copy(b, batch)
			runBatch(b)
			batch = batch[:0]
		}
	}

	// Последний неполный батч (1 пользователь).[attached_file:4][attached_file:5]
	if len(batch) > 0 {
		b := make([]User, len(batch))
		copy(b, batch)
		runBatch(b)
	}

	// Ждём завершения всех запросов к GetMessages.[attached_file:3][attached_file:5]
	wg.Wait()
}

// CheckSpam:
//
//	in  - MsgID
//	out - MsgData{ID, HasSpam}
//
// Ограничивает параллелизм до HasSpamMaxAsyncRequests с помощью пула воркеров.[attached_file:4][attached_file:5]
func CheckSpam(in, out chan interface{}) {

	var wg sync.WaitGroup
	workers := HasSpamMaxAsyncRequests

	// Пул воркеров, каждый читает из in и последовательно вызывает HasSpam.[attached_file:5]
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for v := range in {
				id, ok := v.(MsgID)
				if !ok {
					continue
				}
				has, err := HasSpam(id)
				if err != nil {
					// При ошибке данных не получаем, просто пропускаем.[attached_file:5]
					continue
				}
				out <- MsgData{
					ID:      id,
					HasSpam: has,
				}
			}
		}()
	}

	// Возвращаемся только после завершения всех воркеров, чтобы не писать в закрытый out.[attached_file:3][attached_file:5]
	wg.Wait()
}

// CombineResults:
//
//	in  - MsgData
//	out - string вида "<has_spam> <msg_id>"
//
// Копит все результаты, сортирует по HasSpam (true сначала), затем по ID, и выводит.[attached_file:4][attached_file:3]
func CombineResults(in, out chan interface{}) {

	var data []MsgData

	for v := range in {
		md, ok := v.(MsgData)
		if !ok {
			continue
		}
		data = append(data, md)
	}

	// true идут первыми, затем false; внутри групп — по возрастанию ID.[attached_file:4]
	sort.Slice(data, func(i, j int) bool {
		if data[i].HasSpam != data[j].HasSpam {
			return data[i].HasSpam && !data[j].HasSpam
		}
		return data[i].ID < data[j].ID
	})

	for _, md := range data {
		out <- fmt.Sprintf("%v %v", md.HasSpam, md.ID)
	}
}
