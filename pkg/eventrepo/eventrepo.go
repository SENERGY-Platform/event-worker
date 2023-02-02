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

package eventrepo

import (
	"context"
	"errors"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/eventrepo/cloud"
	"github.com/SENERGY-Platform/event-worker/pkg/eventrepo/fog"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"github.com/SENERGY-Platform/models/go/models"
	"log"
	"runtime/debug"
	"sync"
)

type Interface interface {
	Get(message model.ConsumerMessage) (eventDesc []model.EventMessageDesc, err error)
}

func New(ctx context.Context, wg *sync.WaitGroup, config configuration.Config, notifier Notifier) (result Interface, err error) {
	switch config.Mode {
	case configuration.FogMode:
		env, err := fog.New(ctx, wg, config)
		if err != nil {
			return nil, err
		}
		return &EventRepo[fog.Payload]{env: env, notifier: notifier}, nil
	case configuration.CloudMode:
		env, err := cloud.New(ctx, wg, config)
		if err != nil {
			return nil, err
		}
		return &EventRepo[cloud.Payload]{env: env, notifier: notifier}, nil
	default:
		return nil, errors.New("unknown mode: " + config.Mode)
	}
}

type EventRepo[Payload any] struct {
	env      Environment[Payload]
	notifier Notifier
}

type Notifier interface {
	NotifyError(desc model.EventMessageDesc, err error) error
}

type Environment[Payload any] interface {
	IsServiceMessage(message model.ConsumerMessage) bool
	ParseServiceMessage(message model.ConsumerMessage) (deviceId string, serviceId string, payload Payload, err error)
	GetServiceEventDescriptions(deviceId string, serviceId string) ([]model.EventDesc, error)

	IsImportMessage(message model.ConsumerMessage) bool
	ParseImportMessage(message model.ConsumerMessage) (importId string, payload Payload, err error)
	GetImportEventDescriptions(importId string) ([]model.EventDesc, error)

	SerializeMessage(payload Payload, service models.Service) (result model.SerializedMessage, err error)
}

func (this *EventRepo[Payload]) Get(message model.ConsumerMessage) (result []model.EventMessageDesc, err error) {
	var descriptions []model.EventDesc
	var pl Payload
	if this.env.IsServiceMessage(message) {
		var deviceId string
		var serviceId string
		deviceId, serviceId, pl, err = this.env.ParseServiceMessage(message)
		if err != nil {
			log.Println("WARNING: unable to parse service message", err)
			return nil, nil //ignore message
		}
		descriptions, err = this.env.GetServiceEventDescriptions(deviceId, serviceId)
		if err != nil {
			return nil, err
		}
	} else if this.env.IsImportMessage(message) {
		var importId string
		importId, pl, err = this.env.ParseImportMessage(message)
		if err != nil {
			log.Println("WARNING: unable to parse import message", err)
			return nil, nil //ignore message
		}
		descriptions, err = this.env.GetImportEventDescriptions(importId)
		if err != nil {
			return nil, err
		}
	} else {
		log.Println("WARNING: unknown message type", message.Topic)
		return nil, nil
	}

	for _, desc := range descriptions {
		msgDesc := model.EventMessageDesc{EventDesc: desc}
		msgDesc.Message, err = this.env.SerializeMessage(pl, desc.ServiceForMarshaller)
		if err != nil {
			notifierErr := this.notifier.NotifyError(msgDesc, err)
			if notifierErr != nil {
				log.Println("ERROR:", notifierErr)
				debug.PrintStack()
			}
			continue
		}
		result = append(result, msgDesc)
	}
	return result, nil
}
