/*
 * Copyright (c) 2023 InfAI (CC SES)
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

package worker

import (
	"log"
	"time"
)

func (this *Worker) StartStatistics() {
	ticker := time.NewTicker(time.Minute)
	this.wg.Add(1)
	go func() {
		defer this.wg.Done()
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				this.printStatistics(time.Minute)
			case <-this.ctx.Done():
				return
			}
		}
	}()

}

func (this *Worker) printStatistics(duration time.Duration) {
	this.statMux.Lock()
	defer this.statMux.Unlock()
	log.Printf(
		`STATISTICS: (%v)
    consumed messages: %v from %v topics
    skiped messages (older then %v): %v
    average age: %v seconds
    max age: %v seconds
    event-repo lock-time: %v
    worker.Do() lock-time: %v`,
		duration.String(),
		this.statMsgCount,
		len(this.statTopics),
		this.config.MaxMessageAge,
		this.statSkipCount,
		avg(this.statAges),
		max(this.statAges),
		this.statEventRepoWait.String(),
		this.statDoWait.String())
	this.statTopics = map[string]bool{}
	this.statMsgCount = 0
	this.statAges = []int{}
	this.statEventRepoWait = 0
	this.statDoWait = 0
	this.statDoWait = 0
}

func (this *Worker) logStats(topic string, ageInSec int) {
	this.statMux.Lock()
	defer this.statMux.Unlock()
	this.statTopics[topic] = true
	this.statMsgCount = this.statMsgCount + 1
	this.statAges = append(this.statAges, ageInSec)
}

func (this *Worker) logEventRepoWait(wait time.Duration) {
	this.statMux.Lock()
	defer this.statMux.Unlock()
	this.statEventRepoWait = this.statEventRepoWait + wait
}

func (this *Worker) logDoWait(wait time.Duration) {
	this.statMux.Lock()
	defer this.statMux.Unlock()
	this.statDoWait = this.statDoWait + wait
}

func (this *Worker) logStatsSkip() {
	this.statMux.Lock()
	defer this.statMux.Unlock()
	this.statSkipCount = this.statSkipCount + 1
}

func sum(arr []int) (result int) {
	for _, num := range arr {
		result += num
	}
	return result
}

func avg(arr []int) (result int) {
	l := len(arr)
	if l == 0 {
		l = 1
	}
	return sum(arr) / l
}

func max(arr []int) (result int) {
	for _, num := range arr {
		if num > result {
			result = num
		}
	}
	return result
}
