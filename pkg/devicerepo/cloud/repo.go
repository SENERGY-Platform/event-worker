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
	"errors"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/models/go/models"
	"sync"
)

func New(ctx context.Context, wg *sync.WaitGroup, config configuration.Config) (result *DeviceRepo, err error) {
	//TODO implement me
	panic("implement me")
	return &DeviceRepo{}, errors.New("implement me")
}

type DeviceRepo struct {
}

func (this *DeviceRepo) GetCharacteristic(id string) (characteristic models.Characteristic, err error) {
	//TODO implement me
	panic("implement me")
}

func (this *DeviceRepo) GetConcept(id string) (concept models.Concept, err error) {
	//TODO implement me
	panic("implement me")
}

func (this *DeviceRepo) GetConceptIdOfFunction(id string) string {
	//TODO implement me
	panic("implement me")
}

func (this *DeviceRepo) GetAspectNode(id string) (models.AspectNode, error) {
	//TODO implement me
	panic("implement me")
}
