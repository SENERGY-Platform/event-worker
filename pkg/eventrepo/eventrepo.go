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
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"github.com/SENERGY-Platform/models/go/models"
)

type EventRepo struct{}

func (this *EventRepo) Get(message model.ConsumerMessage) (eventDesc []model.EventMessageDesc, err error) {
	return nil, nil
	//TODO implement me
	panic("implement me")
}

type Payload = interface{}

type Environment interface {
	IsServiceMessage(message model.ConsumerMessage) bool
	IsImportTopic(message model.ConsumerMessage) bool
	ParseServiceMessage(message model.ConsumerMessage) (deviceId string, serviceId string, payload Payload, err error)
	ParseImportMessage(message model.ConsumerMessage) (importId string, payload Payload, err error)
	GetDeviceEventDescriptions(deviceId string, serviceId string) (model.EventDesc, error)
	GetImportEventDescriptions(importId string) (model.EventDesc, error)
	SerializeMessage(payload Payload, service models.Service) (result model.SerializedMessage, err error)
}
