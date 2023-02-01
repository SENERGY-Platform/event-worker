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

package model

import (
	"errors"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/process-deployment/lib/model/deploymentmodel"
)

type DeviceTypeCommand struct {
	Command    string            `json:"command"`
	Id         string            `json:"id"`
	Owner      string            `json:"owner"`
	DeviceType models.DeviceType `json:"device_type"`
}

type DeploymentCommand struct {
	Command    string                      `json:"command"`
	Id         string                      `json:"id"`
	Owner      string                      `json:"owner"`
	Deployment *deploymentmodel.Deployment `json:"deployment"`
	Source     string                      `json:"source,omitempty"`
	Version    int64                       `json:"version"`
}

type EventDesc struct {
	deploymentmodel.ConditionalEvent
	UserId string `json:"user_id"`
}

type EventMessageDesc struct {
	EventDesc

	//set by event repo
	//may be
	//	- the service from EventDesc.ConditionalEvent.Selection.SelectedServiceId
	//	- or an artificial service for EventDesc.ConditionalEvent.Selection.SelectedImportId
	ServiceForMarshaller models.Service `json:"service_for_marshaller"`

	Message           SerializedMessage `json:"message"`             //set by event repo
	MessageAgeSeconds int               `json:"message_age_seconds"` //set by worker
}

type SerializedMessage = map[string]interface{}

var MessageIgnoreError = errors.New("message will be ignored")
