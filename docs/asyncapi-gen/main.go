/*
 * Copyright 2025 InfAI (CC SES)
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

package main

import (
	"flag"
	"fmt"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/eventrepo/cloud"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"log"
	"os"

	"github.com/swaggest/go-asyncapi/reflector/asyncapi-2.4.0"
	"github.com/swaggest/go-asyncapi/spec-2.4.0"
)

//go:generate go run main.go

func main() {
	configLocation := flag.String("config", "../../config.json", "configuration file")
	flag.Parse()

	conf, err := configuration.Load(*configLocation)
	if err != nil {
		log.Fatal("ERROR: unable to load config", err)
	}

	asyncAPI := spec.AsyncAPI{}
	asyncAPI.Info.Title = "Event-Worker"

	asyncAPI.AddServer("kafka", spec.Server{
		URL:      conf.KafkaUrl,
		Protocol: "kafka",
	})

	asyncAPI.AddServer("mqtt", spec.Server{
		URL:      conf.MgwMqttBroker,
		Protocol: "mqtt",
	})

	reflector := asyncapi.Reflector{}
	reflector.Schema = &asyncAPI

	mustNotFail := func(err error) {
		if err != nil {
			panic(err.Error())
		}
	}

	//"topic is a service.Id with replaced '#' and ':' by '_'"
	mustNotFail(reflector.AddChannel(asyncapi.ChannelInfo{
		Name: "Service-Topic",
		Publish: &asyncapi.MessageSample{
			MessageEntity: spec.MessageEntity{
				Name:  "Envelope",
				Title: "Envelope",
			},
			MessageSample: new(cloud.Envelope),
		},
	}))

	//"event/#"
	mustNotFail(reflector.AddChannel(asyncapi.ChannelInfo{
		Name: "event/{device-local-id}/{service-local-id}",
		Publish: &asyncapi.MessageSample{
			MessageEntity: spec.MessageEntity{
				Name:  "payload",
				Title: "payload",
			},
			MessageSample: "",
		},
	}))

	mustNotFail(reflector.AddChannel(asyncapi.ChannelInfo{
		Name: conf.DeviceTypeTopic,
		Publish: &asyncapi.MessageSample{
			MessageEntity: spec.MessageEntity{
				Name:  "DeviceTypeCommand",
				Title: "DeviceTypeCommand",
			},
			MessageSample: new(model.DeviceTypeCommand),
		},
	}))

	mustNotFail(reflector.AddChannel(asyncapi.ChannelInfo{
		Name: conf.ProcessDeploymentDoneTopic,
		Publish: &asyncapi.MessageSample{
			MessageEntity: spec.MessageEntity{
				Name:  "DeploymentDoneNotification",
				Title: "DeploymentDoneNotification",
			},
			MessageSample: new(cloud.DoneNotification),
		},
	}))

	buff, err := reflector.Schema.MarshalJSON()
	mustNotFail(err)

	fmt.Println(string(buff))
	mustNotFail(os.WriteFile("asyncapi.json", buff, 0o600))
}
