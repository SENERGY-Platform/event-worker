/*
 * Copyright 2025 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package trigger

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestTopicMutex(t *testing.T) {
	list := []string{}
	mux := sync.Mutex{}

	now := time.Now()

	task0s := func() {
		mux.Lock()
		defer mux.Unlock()
		since := time.Since(now).Round(time.Second)
		list = append(list, "task0s_"+since.String())
	}
	task1s := func() {
		time.Sleep(1 * time.Second)
		mux.Lock()
		defer mux.Unlock()
		since := time.Since(now).Round(time.Second)
		list = append(list, "task1s_"+since.String())
	}
	alt1s := func() {
		time.Sleep(1 * time.Second)
		mux.Lock()
		defer mux.Unlock()
		since := time.Since(now).Round(time.Second)
		list = append(list, "alt1s_"+since.String())
	}
	task2s := func() {
		time.Sleep(2 * time.Second)
		mux.Lock()
		defer mux.Unlock()
		since := time.Since(now).Round(time.Second)
		list = append(list, "task2s_"+since.String())
	}

	topicmux := TopicMutex{}

	golock := func(wg *sync.WaitGroup, topic string, task func()) {
		defer wg.Done()
		topicmux.Lock(topic)
		defer topicmux.Unlock(topic)
		task()
	}

	wg := &sync.WaitGroup{}
	wg.Add(7)

	go golock(wg, "c", alt1s)
	go golock(wg, "a", task0s)
	go golock(wg, "b", task2s)
	time.Sleep(100 * time.Millisecond) //ensure task2s gets its lock before task1s
	go golock(wg, "b", task1s)
	go golock(wg, "a", task0s)
	go golock(wg, "a", task0s)
	time.Sleep(3 * time.Second)
	go golock(wg, "b", task1s)

	wg.Wait()

	expected := []string{"task0s_0s", "task0s_0s", "task0s_0s", "alt1s_1s", "task2s_2s", "task1s_3s", "task1s_4s"}
	if !reflect.DeepEqual(list, expected) {
		t.Errorf("\na=%#v\ne=%#v\n", list, expected)
	}
}
