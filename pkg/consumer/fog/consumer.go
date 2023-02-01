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
	"fmt"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	paho "github.com/eclipse/paho.mqtt.golang"
	"log"
	"sync"
	"time"
)

type Worker interface {
	Do(topic string, message []byte, ageInSec int) error
}

const TOPIC = "event/#"

func Start(ctx context.Context, wg *sync.WaitGroup, config configuration.Config, worker Worker) error {
	options := paho.NewClientOptions().
		SetPassword(config.MgwMqttPw).
		SetUsername(config.MgwMqttUser).
		SetAutoReconnect(true).
		SetCleanSession(true).
		SetClientID(config.MgwMqttClientId).
		AddBroker(config.MgwMqttBroker).
		SetWriteTimeout(10 * time.Second).
		SetOrderMatters(false).
		SetResumeSubs(true).
		SetConnectionLostHandler(func(_ paho.Client, err error) {
			log.Println("connection to mgw broker lost")
		}).
		SetOnConnectHandler(func(client paho.Client) {
			log.Println("connected to mgw broker")
			token := client.Subscribe(TOPIC, config.MgwMqttQos, func(client paho.Client, message paho.Message) {
				err := worker.Do(message.Topic(), message.Payload(), 0)
				if err != nil {
					log.Println("ERROR:", err)
				}
			})
			if token.Wait() && token.Error() != nil {
				config.HandleFatalError(fmt.Sprintf("FATAL-ERROR: unable to subscribe to %v: %v", TOPIC, token.Error()))
				return
			}
		})

	client := paho.NewClient(options)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Println("ERROR: unable to connect to mgw broker", token.Error())
		return token.Error()
	}

	wg.Add(1)
	go func() {
		<-ctx.Done()
		client.Disconnect(0)
		wg.Done()
	}()
	return nil
}
