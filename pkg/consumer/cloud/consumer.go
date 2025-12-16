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

package cloud

import (
	"context"
	"encoding/json"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
)

type Worker interface {
	Do(msg model.ConsumerMessage) error
	HandleDeploymentUpdateSignal()
}

func Start(basectx context.Context, wg *sync.WaitGroup, config configuration.Config, worker Worker) error {
	ctx, cancel := context.WithCancel(basectx)
	consumer := NewUpdatableConsumer(ctx, config, func(msg model.ConsumerMessage) error {
		return worker.Do(msg)
	}, func(topic string, err error) {
		config.HandleFatalError(err)
	})

	mux := sync.Mutex{}
	update := func() error {
		mux.Lock()
		defer mux.Unlock()
		newTopics, err := GetWorkerTopics(config)
		if err != nil {
			return err
		}
		newTopicsSlice := sliceTopics(config, newTopics)
		return consumer.UpdateTopics(newTopicsSlice)
	}

	err := update()
	if err != nil {
		cancel()
		return err
	}

	updateSignalConsumerGroup := ""
	if config.InstanceId != "" && config.InstanceId != "-" {
		updateSignalConsumerGroup = config.KafkaConsumerGroup + "_" + config.InstanceId
	}

	//update signal: potentially new service
	err = NewKafkaLastOffsetConsumer(basectx, wg, config.KafkaUrl, updateSignalConsumerGroup, config.DeviceTypeTopic, func(delivery []byte) error {
		config.GetLogger().Debug("received device-type update, wait some time before consumer update to ensure that new topics are available", "delay", config.DeviceTypeUpdateTriggerDelaySeconds)
		time.Sleep(time.Duration(config.DeviceTypeUpdateTriggerDelaySeconds) * time.Second)
		dtCmd := model.DeviceTypeCommand{}
		err = json.Unmarshal(delivery, &dtCmd)
		if err != nil {
			config.GetLogger().Warn("unable to interpret msg as device-type update", "error", err)
			return nil //ignore unknown msg format
		}
		if dtCmd.Command != "PUT" {
			return nil //ignore
		}
		return update()
	}, func(err error) {
		config.HandleFatalError(err)
	})
	if err != nil {
		cancel()
		return err
	}

	//update signal: potentially new import
	err = NewKafkaLastOffsetConsumer(basectx, wg, config.KafkaUrl, updateSignalConsumerGroup, config.ProcessDeploymentDoneTopic, func(delivery []byte) error {
		config.GetLogger().Debug("received process-deployment update, wait some time before consumer update to ensure that new topics are available")
		worker.HandleDeploymentUpdateSignal()
		return update()
	}, func(err error) {
		config.HandleFatalError(err)
	})
	if err != nil {
		cancel()
		return err
	}

	return nil
}

func listChanged(a []string, b []string) bool {
	sort.Strings(a)
	sort.Strings(b)
	return !reflect.DeepEqual(a, b)
}

func ServiceIdToTopic(id string) string {
	id = strings.ReplaceAll(id, "#", "_")
	id = strings.ReplaceAll(id, ":", "_")
	return id
}

func ImportIdToTopic(id string) string {
	id = strings.ReplaceAll(id, "#", "_")
	id = strings.ReplaceAll(id, ":", "_")
	return id
}

func GetWorkerTopics(config configuration.Config) (result []string, err error) {
	allServicesAndImportsRegex := regexp.MustCompile("urn_infai_ses_service_.*|urn_infai_ses_import_.*")
	return GetTopics(config.KafkaUrl, allServicesAndImportsRegex)
}

func sliceTopics(config configuration.Config, list []string) (result []string) {
	if config.KafkaTopicSliceCount <= 1 {
		return list
	}
	sort.Strings(list)
	used := config.KafkaTopicSliceIndex % config.KafkaTopicSliceCount
	for i, topic := range list {
		if i%config.KafkaTopicSliceCount == used {
			result = append(result, topic)
		}
	}
	return result
}
