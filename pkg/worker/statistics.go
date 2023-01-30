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
				this.printStatistics()
			case <-this.ctx.Done():
				return
			}
		}
	}()

}

func (this *Worker) printStatistics() {
	this.statMux.Lock()
	defer this.statMux.Unlock()
	log.Printf("STATISTICS: consumed %v messages from %v topics\n", this.statMsgCount, len(this.statTopics))
	this.statTopics = map[string]bool{}
	this.statMsgCount = 0
}

func (this *Worker) logStats(topic string) {
	this.statMux.Lock()
	defer this.statMux.Unlock()
	this.statTopics[topic] = true
	this.statMsgCount = this.statMsgCount + 1
}
