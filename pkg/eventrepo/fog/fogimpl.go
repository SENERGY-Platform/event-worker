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

package fog

import (
	"context"
	"errors"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"github.com/SENERGY-Platform/models/go/models"
	"sync"
)

type Payload = interface{}

func New(ctx context.Context, wg *sync.WaitGroup, config configuration.Config) (result *Impl, err error) {
	return &Impl{}, nil
}

type Impl struct{}

func (this *Impl) IsServiceMessage(message model.ConsumerMessage) bool {
	//TODO implement me
	panic("implement me")
}

func (this *Impl) IsImportMessage(message model.ConsumerMessage) bool {
	return false
}

func (this *Impl) ParseServiceMessage(message model.ConsumerMessage) (deviceId string, serviceId string, payload Payload, err error) {
	//TODO implement me
	panic("implement me")
}

func (this *Impl) ParseImportMessage(message model.ConsumerMessage) (importId string, payload Payload, err error) {
	err = errors.New("import messages not supported in fog mode")
	return
}

func (this *Impl) GetServiceEventDescriptions(deviceId string, serviceId string) ([]model.EventDesc, error) {
	//TODO implement me
	panic("implement me")
}

func (this *Impl) GetImportEventDescriptions(importId string) ([]model.EventDesc, error) {
	return nil, errors.New("import messages not supported in fog mode")
}

func (this *Impl) SerializeMessage(payload Payload, service models.Service) (result model.SerializedMessage, err error) {
	//TODO implement me
	panic("implement me")
}
