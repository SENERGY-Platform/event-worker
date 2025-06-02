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
	consumer "github.com/SENERGY-Platform/event-worker/pkg/consumer/cloud"
	"github.com/SENERGY-Platform/event-worker/pkg/eventrepo/cloud/mongo"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/service-commons/pkg/cache"
	"log"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

type Payload = map[string]interface{}

func New(ctx context.Context, wg *sync.WaitGroup, config configuration.Config) (result *Impl, err error) {
	m, err := mongo.New(ctx, wg, config)
	if err != nil {
		return result, err
	}
	result = &Impl{
		config: config,
		db:     m,
	}

	if config.CloudEventRepoCacheDuration != "" && config.CloudEventRepoCacheDuration != "-" {
		cacheDuration, err := time.ParseDuration(config.CloudEventRepoCacheDuration)
		if err != nil {
			return result, err
		}
		result.cacheDuration = cacheDuration
		result.cache, err = cache.New(cache.Config{})
		if err != nil {
			return result, err
		}
	}

	if config.KafkaUrl != "" && config.KafkaUrl != "-" && config.ProcessDeploymentDoneTopic != "" && config.ProcessDeploymentDoneTopic != "-" {
		log.Println("watch deployments done topic:", config.ProcessDeploymentDoneTopic)
		err = result.watchDeploymentsDoneToResetCache(ctx, wg)
		if err != nil {
			return result, err
		}
	} else {
		log.Printf("watch deployments done topic: disabled because config.KafkaUrl='%v' config.ProcessDeploymentDoneTopic='%v'\n", config.KafkaUrl, config.ProcessDeploymentDoneTopic)
	}

	return result, nil
}

type Impl struct {
	config        configuration.Config
	db            *mongo.Mongo
	cache         *cache.Cache
	cacheDuration time.Duration
}

func (this *Impl) ResetCache() {
	this.cache.Reset()
}

// urn_infai_ses_service_557a8519-c801-42c6-a5e0-d6d6450ec9ab
func (this *Impl) IsServiceMessage(message model.ConsumerMessage) bool {
	return strings.HasPrefix(message.Topic, "urn_infai_ses_service_")
}

// urn_infai_ses_import_7f2620cb-002c-fc54-0c2e-5e840b7b0263
func (this *Impl) IsImportMessage(message model.ConsumerMessage) bool {
	return strings.HasPrefix(message.Topic, "urn_infai_ses_import_")
}

func (this *Impl) ParseServiceMessage(message model.ConsumerMessage) (deviceId string, serviceId string, payload Payload, err error) {
	envelope := Envelope{}
	err = json.Unmarshal(message.Message, &envelope)
	if err != nil {
		return
	}
	deviceId = envelope.DeviceId
	serviceId = envelope.ServiceId
	payload = envelope.Value
	return
}

func (this *Impl) ParseImportMessage(message model.ConsumerMessage) (importId string, payload Payload, err error) {
	envelope := ImportEnvelope{}
	err = json.Unmarshal(message.Message, &envelope)
	if err != nil {
		return
	}
	importId = envelope[ImportEnvelopeIdField].(string)
	payload = envelope
	return
}

func (this *Impl) GetServiceEventDescriptions(deviceId string, serviceId string) (result []model.EventDesc, err error) {
	//cache.Use catches nil this.cache
	return cache.Use(this.cache, "events.device_service."+deviceId+"."+serviceId, func() (result []model.EventDesc, err error) {
		return this.db.GetEventDescriptionsByDeviceAndService(deviceId, serviceId)
	}, func(descs []model.EventDesc) error {
		return nil
	}, this.cacheDuration)
}

func (this *Impl) GetImportEventDescriptions(importId string) (result []model.EventDesc, err error) {
	//cache.Use catches nil this.cache
	return cache.Use(this.cache, "events.import."+importId, func() (result []model.EventDesc, err error) {
		return this.db.GetEventDescriptionsByImportId(importId)
	}, func(descs []model.EventDesc) error {
		return nil
	}, this.cacheDuration)
}

func (this *Impl) SerializeMessage(payload Payload, service models.Service) (result model.SerializedMessage, err error) {
	return payload, nil
}

type Envelope struct {
	DeviceId  string                 `json:"device_id,omitempty"`
	ServiceId string                 `json:"service_id,omitempty"`
	Value     map[string]interface{} `json:"value"`
}

type ImportEnvelope = map[string]interface{}

const ImportEnvelopeIdField = "import_id"

func (this *Impl) watchDeploymentsDoneToResetCache(ctx context.Context, wg *sync.WaitGroup) error {
	updateSignalConsumerGroup := ""
	if this.config.InstanceId != "" && this.config.InstanceId != "-" {
		updateSignalConsumerGroup = this.config.KafkaConsumerGroup + "_" + this.config.InstanceId
	}
	return consumer.NewKafkaLastOffsetConsumer(ctx, wg, this.config.KafkaUrl, updateSignalConsumerGroup, this.config.ProcessDeploymentDoneTopic, func(delivery []byte) error {
		msg := DoneNotification{}
		err := json.Unmarshal(delivery, &msg)
		if err != nil {
			log.Println("ERROR: unable to interpret kafka msg:", err)
			debug.PrintStack()
			return nil //ignore  message
		}
		log.Println("receive deployment done message:", msg)
		if msg.Handler == this.config.WatchedProcessDeploymentDoneHandler {
			this.ResetCache()
		}
		return nil
	}, func(err error) {
		this.config.HandleFatalError(err)
	})
}

type DoneNotification struct {
	Command string `json:"command"`
	Id      string `json:"id"`
	Handler string `json:"handler"`
}
