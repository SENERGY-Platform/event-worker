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
)

type DeviceTypeCommand struct {
	Command    string            `json:"command"`
	Id         string            `json:"id"`
	Owner      string            `json:"owner"`
	DeviceType models.DeviceType `json:"device_type"`
}

type DeploymentCommand struct {
	Command    string             `json:"command"`
	Id         string             `json:"id"`
	Owner      string             `json:"owner"`
	Deployment *models.Deployment `json:"deployment"`
	Source     string             `json:"source,omitempty"`
	Version    int64              `json:"version"`
}

type ConsumerMessage struct {
	Topic    string `json:"topic"`
	Message  []byte `json:"message"`
	AgeInSec int    `json:"age_in_sec"`
	MsgId    string `json:"msg_id"`
}

type EventDesc struct {
	UserId string `json:"user_id" bson:"user_id"`

	//search info
	DeploymentId  string `json:"deployment_id" bson:"deployment_id"`
	DeviceGroupId string `json:"device_group_id" bson:"device_group_id"`
	DeviceId      string `json:"device_id" bson:"device_id"`
	ServiceId     string `json:"service_id" bson:"service_id"`
	ImportId      string `json:"import_id" bson:"import_id"`

	//worker info
	Script        string            `json:"script" bson:"script"`
	ValueVariable string            `json:"value_variable" bson:"value_variable"`
	Variables     map[string]string `json:"variables" bson:"variables"`
	Qos           int               `json:"qos" bson:"qos"`
	EventId       string            `json:"event_id" bson:"event_id"`

	//marshaller info
	CharacteristicId string `json:"characteristic_id" bson:"characteristic_id"`
	FunctionId       string `json:"function_id" bson:"function_id"`
	AspectId         string `json:"aspect_id" bson:"aspect_id"`
	Path             string `json:"path" bson:"path"`

	//set by event-manager
	//may be
	//	- the service from EventDesc.ConditionalEvent.Selection.SelectedServiceId
	//	- or an artificial service for EventDesc.ConditionalEvent.Selection.SelectedImportId
	ServiceForMarshaller models.Service `json:"service_for_marshaller" bson:"service_for_marshaller"`
}

type EventMessageDesc struct {
	EventDesc                           //read from storage
	Message           SerializedMessage `json:"message"`             //set by event repo by serializing message
	MessageAgeSeconds int               `json:"message_age_seconds"` //set by worker
	MessageId         string            `json:"message_id"`          //set by worker
}

type SerializedMessage = map[string]interface{}

var MessageIgnoreError = errors.New("message will be ignored")
