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

package configuration

import (
	"log"
	"strconv"
	"strings"
)

func (this Config) LogTrace(key string, values ...interface{}) {
	if this.DebugTraceIndex[key] {
		msg := []interface{}{}
		msg = append(msg, values...)
		log.Printf("TRACE: %v, %#v\n", key, msg)
	}
}

func handleInstanceIdAsSliceIndex(config *Config) {
	if config.InstanceId != "" && config.InstanceId != "-" {
		parts := strings.Split(config.InstanceId, "-")
		lastPart := parts[len(parts)-1]
		index, err := strconv.Atoi(lastPart)
		if err == nil {
			log.Println("use instance_id suffix as kafka_topic_slice_index", index)
			config.KafkaTopicSliceIndex = index
		}
	}
	return
}

func setTraceIndex(config *Config) {
	config.DebugTraceIndex = map[string]bool{}
	for _, v := range config.DebugTraceKeys {
		config.DebugTraceIndex[v] = true
	}
}
