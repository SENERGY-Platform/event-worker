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
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/process-deployment/lib/model/deploymentmodel"
	"log"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
)

type Worker interface {
	Do(msg model.ConsumerMessage) error
}

func Start(basectx context.Context, wg *sync.WaitGroup, config configuration.Config, worker Worker) error {
	ctx, cancel := context.WithCancel(basectx)

	currentTopics, err := GetWorkerTopics(config)
	if err != nil {
		cancel()
		return err
	}
	currentTopicSlice := sliceTopics(config, currentTopics)

	err = startWithServiceIds(ctx, wg, config, worker, currentTopicSlice)
	if err != nil {
		cancel()
		return err
	}

	mux := sync.Mutex{}

	updateSignalConsumerGroup := ""
	if config.InstanceId != "" && config.InstanceId != "-" {
		updateSignalConsumerGroup = config.KafkaConsumerGroup + "_" + config.InstanceId
	}

	//update signal: potentially new service
	err = NewKafkaLastOffsetConsumer(basectx, wg, config.KafkaUrl, updateSignalConsumerGroup, config.DeviceTypeTopic, func(delivery []byte) error {
		mux.Lock()
		defer mux.Unlock()
		dtCmd := model.DeviceTypeCommand{}
		err = json.Unmarshal(delivery, &dtCmd)
		if err != nil {
			log.Println("WARNING: unable to interpret msg as device-type update:\n\t", string(delivery), "\n\t", err)
			return nil //ignore unknown msg format
		}
		if dtCmd.Command != "PUT" {
			return nil //ignore
		}
		newTopics, err := GetWorkerTopics(config)
		if err != nil {
			return err
		}
		newTopics, addedServiceTopics := addDtServices(newTopics, dtCmd.DeviceType)
		newTopicsSlice := sliceTopics(config, newTopics)
		if listChanged(currentTopicSlice, newTopicsSlice) {
			log.Println("update service topic consumer")
			err = InitTopics(config.KafkaUrl, config.ServiceTopicConfig, addedServiceTopics...)
			if err != nil {
				return err
			}
			newCtx, newCancel := context.WithCancel(basectx)
			err = startWithServiceIds(newCtx, wg, config, worker, newTopicsSlice)
			if err != nil {
				newCancel()
				return err
			}
			cancel()
			currentTopics, currentTopicSlice, ctx, cancel = newTopics, newTopicsSlice, newCtx, newCancel
		}
		return nil
	}, func(err error) {
		config.HandleFatalError(err)
	})
	if err != nil {
		cancel()
		return err
	}

	//update signal: potentially new import
	err = NewKafkaLastOffsetConsumer(basectx, wg, config.KafkaUrl, updateSignalConsumerGroup, config.ProcessDeploymentTopic, func(delivery []byte) error {
		mux.Lock()
		defer mux.Unlock()
		processDeplCmd := model.DeploymentCommand{}
		err = json.Unmarshal(delivery, &processDeplCmd)
		if err != nil {
			log.Println("WARNING: unable to interpret msg as process-deployment update:\n\t", string(delivery), "\n\t", err)
			return nil //ignore unknown msg format
		}
		if processDeplCmd.Command != "PUT" {
			return nil //ignore
		}
		if !processContainsNewImport(currentTopics, processDeplCmd.Deployment) {
			return nil //ignore
		}
		newTopics, err := GetWorkerTopics(config)
		if err != nil {
			return err
		}
		newTopics = mergeLists(currentTopics, newTopics)
		newTopicsSlice := sliceTopics(config, newTopics)
		if listChanged(currentTopicSlice, newTopicsSlice) {
			log.Println("update service topic consumer")
			newCtx, newCancel := context.WithCancel(basectx)
			err = startWithServiceIds(newCtx, wg, config, worker, newTopicsSlice)
			if err != nil {
				newCancel()
				return err
			}
			cancel()
			currentTopics, currentTopicSlice, ctx, cancel = newTopics, newTopicsSlice, newCtx, newCancel
		}
		return nil
	}, func(err error) {
		config.HandleFatalError(err)
	})
	if err != nil {
		cancel()
		return err
	}

	return nil
}

func processContainsNewImport(knownTopics []string, deployment *deploymentmodel.Deployment) bool {
	if deployment == nil {
		return false
	}
	knownTopicIndex := map[string]bool{}
	for _, topic := range knownTopics {
		knownTopicIndex[topic] = true
	}
	for _, element := range deployment.Elements {
		if element.ConditionalEvent != nil &&
			element.ConditionalEvent.Selection.SelectedImportId != nil &&
			!knownTopicIndex[ImportIdToTopic(*element.ConditionalEvent.Selection.SelectedImportId)] {
			return true
		}
	}
	return false
}

func mergeLists(a []string, b []string) (result []string) {
	index := map[string]bool{}
	for _, topic := range a {
		index[topic] = true
	}
	for _, topic := range b {
		index[topic] = true
	}
	for topic, _ := range index {
		result = append(result, topic)
	}
	sort.Strings(result)
	return result
}

func addDtServices(topics []string, deviceType models.DeviceType) (all []string, added []string) {
	index := map[string]bool{}
	for _, topic := range topics {
		index[topic] = true
	}
	for _, service := range deviceType.Services {
		topic := ServiceIdToTopic(service.Id)
		if !index[topic] {
			added = append(all, topic)
		}
		index[topic] = true
	}
	for topic, _ := range index {
		all = append(all, topic)
	}
	sort.Strings(all)
	return all, added
}

func startWithServiceIds(basectx context.Context, wg *sync.WaitGroup, config configuration.Config, worker Worker, serviceIds []string) (err error) {
	topics := []string{}
	for _, id := range serviceIds {
		topics = append(topics, ServiceIdToTopic(id))
	}
	return NewKafkaLastOffsetConsumerGroup(basectx, wg, config.KafkaUrl, config.KafkaConsumerGroup, topics, func(msg model.ConsumerMessage) error {
		return worker.Do(msg)
	}, func(topic string, err error) {
		config.HandleFatalError(err)
	})
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
