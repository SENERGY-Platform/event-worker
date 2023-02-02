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

package marshaller

import (
	"context"
	"fmt"
	"github.com/SENERGY-Platform/converter/lib/converter"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	marshallerconfig "github.com/SENERGY-Platform/marshaller/lib/config"
	"github.com/SENERGY-Platform/marshaller/lib/marshaller/v2"
	"github.com/SENERGY-Platform/models/go/models"
	"log"
	"runtime/debug"
	"sync"
)

type Marshaller struct {
	config     configuration.Config
	marshaller *v2.Marshaller
	deviceRepo DeviceRepo
}

func New(ctx context.Context, wg *sync.WaitGroup, config configuration.Config, deviceRepo DeviceRepo) (result *Marshaller, err error) {
	c, err := converter.New()
	if err != nil {
		return result, err
	}
	result = &Marshaller{
		config:     config,
		marshaller: v2.New(marshallerconfig.Config{Debug: config.Debug}, c, deviceRepo),
		deviceRepo: deviceRepo,
	}
	return result, nil
}

type DeviceRepo interface {
	GetCharacteristic(id string) (characteristic models.Characteristic, err error)
	GetConcept(id string) (concept models.Concept, err error)
	GetConceptIdOfFunction(id string) string
	GetAspectNode(id string) (models.AspectNode, error)
}

func (this *Marshaller) Unmarshal(desc model.EventMessageDesc) (value interface{}, err error) {
	service := desc.ServiceForMarshaller
	characteristicId := desc.CharacteristicId

	var path = desc.Path
	if path == "" {
		path, err = this.getPath(desc, service)
	}

	//no protocol is needed because we provide a serialized message
	value, err = this.marshaller.Unmarshal(models.Protocol{}, service, characteristicId, path, nil, desc.Message)
	if err != nil {
		err = fmt.Errorf("%w: %v", model.MessageIgnoreError, err.Error())
	}
	return value, err
}

func (this *Marshaller) getPath(desc model.EventMessageDesc, service models.Service) (string, error) {
	if desc.AspectId == "" {
		return "", fmt.Errorf("%w: %v", model.MessageIgnoreError, "missing aspect id in conditional event description")
	}
	if desc.FunctionId == "" {
		return "", fmt.Errorf("%w: %v", model.MessageIgnoreError, "missing function id in conditional event description")
	}
	aspect, err := this.deviceRepo.GetAspectNode(desc.AspectId)
	if err != nil {
		return "", err
	}
	paths := this.marshaller.GetOutputPaths(service, desc.FunctionId, &aspect)
	if len(paths) > 1 {
		paths, err = this.marshaller.SortPathsByAspectDistance(this.deviceRepo, service, &aspect, paths)
		if err != nil {
			log.Println("ERROR:", err)
			debug.PrintStack()
			return "", fmt.Errorf("%w: %v", model.MessageIgnoreError, err.Error())
		}
		if this.config.Debug {
			log.Println("WARNING: only one path found by FunctionId and AspectNode is used for Unmarshal")
		}
	}
	if len(paths) == 0 {
		return "", fmt.Errorf("%w: %v", model.MessageIgnoreError, "no output path found for criteria")
	}
	return paths[0], nil
}
