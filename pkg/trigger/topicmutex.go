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

import "sync"

type TopicMutex struct {
	global sync.Mutex
	count  map[string]int
	mux    map[string]*sync.Mutex
}

func (this *TopicMutex) Lock(topic string) {
	this.global.Lock()
	if this.count == nil {
		this.count = map[string]int{}
	}
	if this.mux == nil {
		this.mux = map[string]*sync.Mutex{}
	}
	this.count[topic] = this.count[topic] + 1
	if mux, ok := this.mux[topic]; ok {
		this.global.Unlock()
		mux.Lock()
	} else {
		mux = &sync.Mutex{}
		this.mux[topic] = mux
		mux.Lock()
		this.global.Unlock()
	}
}

func (this *TopicMutex) Unlock(topic string) {
	this.global.Lock()
	defer this.global.Unlock()
	count := this.count[topic] - 1
	this.count[topic] = count
	if count < 1 {
		delete(this.count, topic)
	}
	if mux, ok := this.mux[topic]; ok {
		if count < 1 {
			delete(this.mux, topic)
		}
		mux.Unlock()
	}
}
