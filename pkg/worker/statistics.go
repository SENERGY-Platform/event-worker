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
	"github.com/SENERGY-Platform/event-worker/pkg/metrics"
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
	this.metrics = metrics.New().Serve(this.ctx, this.config.MetricsPort)
}

func (this *Worker) printStatistics(duration time.Duration) {
	this.statMux.Lock()
	defer this.statMux.Unlock()
	log.Printf(
		`STATISTICS: (%v)
|        received deployment update signals: %v
|        consumed messages: %v from %v topics
|        skiped messages (older than %v): %v
|        scripts run: %v
|        events triggered: %v
|        average age: %v seconds
|        max age: %v seconds
|        event-repo lock-time: %v
|        worker.Do() lock-time: %v
-------------------------------------------------`,
		duration.String(),
		this.deploymentUpdateSignalCount,
		this.statMsgCount,
		len(this.statTopics),
		this.config.MaxMessageAge,
		this.statSkipCount,
		this.statScriptCount,
		this.statTriggerCount,
		avg(this.statAges),
		max(this.statAges),
		this.statEventRepoWait.String(),
		this.statDoWait.String())
	this.statTopics = map[string]bool{}
	this.statMsgCount = 0
	this.statAges = []int{}
	this.statEventRepoWait = 0
	this.statDoWait = 0
	this.statSkipCount = 0
	this.statScriptCount = 0
	this.statTriggerCount = 0
	this.deploymentUpdateSignalCount = 0
}

func (this *Worker) logStats(topic string, ageInSec int) {
	this.statMux.Lock()
	defer this.statMux.Unlock()
	this.statTopics[topic] = true
	this.statMsgCount = this.statMsgCount + 1
	this.statAges = append(this.statAges, ageInSec)
	this.metrics.ConsumedMessages.Inc()
	this.metrics.MessageAges.Observe(float64(ageInSec))
}

func (this *Worker) logEventRepoWait(wait time.Duration) {
	this.statMux.Lock()
	defer this.statMux.Unlock()
	this.statEventRepoWait = this.statEventRepoWait + wait
	this.metrics.EventRepoLockTime.Add(float64(wait.Milliseconds()))
}

func (this *Worker) logDoWait(wait time.Duration) {
	this.statMux.Lock()
	defer this.statMux.Unlock()
	this.statDoWait = this.statDoWait + wait
	this.metrics.DoDuration.Add(float64(wait.Milliseconds()))
}

func (this *Worker) logStatsSkip() {
	this.statMux.Lock()
	defer this.statMux.Unlock()
	this.statSkipCount = this.statSkipCount + 1
	this.metrics.MessagesSkipped.Inc()
}

func (this *Worker) logScript() {
	this.statMux.Lock()
	defer this.statMux.Unlock()
	this.statScriptCount = this.statScriptCount + 1
	this.metrics.EventsChecked.Inc()
}

func (this *Worker) logDeploymentUpdateSignal() {
	this.statMux.Lock()
	defer this.statMux.Unlock()
	this.deploymentUpdateSignalCount = this.deploymentUpdateSignalCount + 1
	this.metrics.DeploymentUpdateSignalCount.Inc()
}

func (this *Worker) logTrigger() {
	this.statMux.Lock()
	defer this.statMux.Unlock()
	this.statTriggerCount = this.statTriggerCount + 1
	this.metrics.EventsTriggered.Inc()
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
