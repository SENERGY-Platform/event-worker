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
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"github.com/SENERGY-Platform/event-worker/pkg/tests/docker"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/process-deployment/lib/config"
	"github.com/SENERGY-Platform/process-deployment/lib/model/deploymentmodel"
	"github.com/ory/dockertest/v3"
	"github.com/segmentio/kafka-go"
	"log"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestConsumerUpdateSignal(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := configuration.Load("../../../config.json")
	if err != nil {
		t.Error(err)
		return
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Error(err)
		return
	}

	_, zkIp, err := docker.Zookeeper(pool, ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}
	zookeeperUrl := zkIp + ":2181"

	config.KafkaUrl, err = docker.Kafka(pool, ctx, wg, zookeeperUrl)
	if err != nil {
		t.Error(err)
		return
	}

	initialServiceTopic := "urn_infai_ses_service_1"
	initialImportTopic := "urn_infai_ses_import_1"
	err = InitTopics(config.KafkaUrl, config.ServiceTopicConfig, config.DeviceTypeTopic, config.ProcessDeploymentTopic, initialServiceTopic, initialImportTopic)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(10 * time.Second)

	messages := map[string][]string{}

	err = Start(ctx, wg, config, MockWorker{func(topic string, message []byte) error {
		messages[topic] = append(messages[topic], string(message))
		return nil
	}})
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(10 * time.Second)

	deviceTypeProducer, err := NewTestProducer(config.KafkaUrl, config.DeviceTypeTopic)
	if err != nil {
		t.Error(err)
		return
	}

	deploymentProducer, err := NewTestProducer(config.KafkaUrl, config.ProcessDeploymentTopic)
	if err != nil {
		t.Error(err)
		return
	}

	service1Producer, err := NewTestProducer(config.KafkaUrl, initialServiceTopic)
	if err != nil {
		t.Error(err)
		return
	}

	import1Producer, err := NewTestProducer(config.KafkaUrl, initialImportTopic)
	if err != nil {
		t.Error(err)
		return
	}

	for _, m := range []string{"1", "2", "3"} {
		err = service1Producer(m)
		if err != nil {
			t.Error(err)
			return
		}
		err = import1Producer(m)
		if err != nil {
			t.Error(err)
			return
		}
	}

	import2Topic := "urn_infai_ses_import_2"
	err = InitTopics(config.KafkaUrl, config.ServiceTopicConfig, import2Topic)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(10 * time.Second)

	service2Topic := "urn_infai_ses_service_2"
	dtMsg, _ := json.Marshal(model.DeviceTypeCommand{
		Command: "PUT",
		DeviceType: models.DeviceType{
			Services: []models.Service{{
				Id: "urn:infai:ses:service:2",
			}},
		},
	})
	err = deviceTypeProducer(string(dtMsg))
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(10 * time.Second)

	service2Producer, err := NewTestProducer(config.KafkaUrl, service2Topic)
	if err != nil {
		t.Error(err)
		return
	}

	import2Producer, err := NewTestProducer(config.KafkaUrl, import2Topic)
	if err != nil {
		t.Error(err)
		return
	}

	for _, m := range []string{"4", "5", "6"} {
		err = service1Producer(m)
		if err != nil {
			t.Error(err)
			return
		}
		err = import1Producer(m)
		if err != nil {
			t.Error(err)
			return
		}
		err = service2Producer(m)
		if err != nil {
			t.Error(err)
			return
		}
		err = import2Producer(m)
		if err != nil {
			t.Error(err)
			return
		}
	}

	service3Topic := "urn_infai_ses_service_3"
	err = InitTopics(config.KafkaUrl, config.ServiceTopicConfig, service3Topic)
	if err != nil {
		t.Error(err)
		return
	}
	import3Topic := "urn_infai_ses_import_3"
	err = InitTopics(config.KafkaUrl, config.ServiceTopicConfig, import3Topic)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(10 * time.Second)

	import3Id := "urn:infai:ses:3"
	processDeplMsg, _ := json.Marshal(model.DeploymentCommand{
		Command: "PUT",
		Deployment: &deploymentmodel.Deployment{Elements: []deploymentmodel.Element{
			{
				ConditionalEvent: &deploymentmodel.ConditionalEvent{
					Selection: deploymentmodel.Selection{SelectedImportId: &import3Id},
				},
			},
		}},
	})
	err = deploymentProducer(string(processDeplMsg))
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(10 * time.Second)

	service3Producer, err := NewTestProducer(config.KafkaUrl, service3Topic)
	if err != nil {
		t.Error(err)
		return
	}

	import3Producer, err := NewTestProducer(config.KafkaUrl, import3Topic)
	if err != nil {
		t.Error(err)
		return
	}

	for _, m := range []string{"7", "8", "9"} {
		err = service1Producer(m)
		if err != nil {
			t.Error(err)
			return
		}
		err = import1Producer(m)
		if err != nil {
			t.Error(err)
			return
		}
		err = service2Producer(m)
		if err != nil {
			t.Error(err)
			return
		}
		err = import2Producer(m)
		if err != nil {
			t.Error(err)
			return
		}
		err = service3Producer(m)
		if err != nil {
			t.Error(err)
			return
		}
		err = import3Producer(m)
		if err != nil {
			t.Error(err)
			return
		}
	}

	time.Sleep(10 * time.Second)

	expected := map[string][]string{
		initialServiceTopic: {"1", "2", "3", "4", "5", "6", "7", "8", "9"},
		initialImportTopic:  {"1", "2", "3", "4", "5", "6", "7", "8", "9"},
		service2Topic:       {"4", "5", "6", "7", "8", "9"},
		import2Topic:        {"4", "5", "6", "7", "8", "9"},
		service3Topic:       {"7", "8", "9"},
		import3Topic:        {"7", "8", "9"},
	}
	if !reflect.DeepEqual(messages, expected) {
		t.Errorf("\n%#v\n%#v", messages, expected)
	}
}

func NewTestProducer(kafkaUrl string, topic string) (func(msg string) error, error) {
	broker, err := GetBroker(kafkaUrl)
	if err != nil {
		return nil, err
	}
	if len(broker) == 0 {
		return nil, errors.New("missing kafka broker")
	}
	writer := &kafka.Writer{
		Addr:        kafka.TCP(broker...),
		Topic:       topic,
		MaxAttempts: 10,
		BatchSize:   1,
		Balancer:    &kafka.Hash{},
	}
	return func(msg string) error {
		log.Println("produce to", topic)
		return writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(msg),
			Value: []byte(msg),
			Time:  config.TimeNow(),
		})
	}, nil
}

type MockWorker struct {
	F func(topic string, message []byte) error
}

func (this MockWorker) Do(msg model.ConsumerMessage) error {
	return this.F(msg.Topic, msg.Message)
}

func TestSliceTopics(t *testing.T) {
	type args struct {
		config configuration.Config
		list   []string
	}
	topics := []string{}
	for i := 0; i < 10; i++ {
		topics = append(topics, strconv.Itoa(i))
	}
	tests := []struct {
		KafkaTopicSliceCount int
		KafkaTopicSliceIndex int
		wantResult           []string
	}{
		{
			KafkaTopicSliceCount: 0,
			KafkaTopicSliceIndex: 0,
			wantResult:           topics,
		},
		{
			KafkaTopicSliceCount: 1,
			KafkaTopicSliceIndex: 0,
			wantResult:           topics,
		},
		{
			KafkaTopicSliceCount: 2,
			KafkaTopicSliceIndex: 0,
			wantResult:           []string{"0", "2", "4", "6", "8"},
		},
		{
			KafkaTopicSliceCount: 2,
			KafkaTopicSliceIndex: 1,
			wantResult:           []string{"1", "3", "5", "7", "9"},
		},
		{
			KafkaTopicSliceCount: 2,
			KafkaTopicSliceIndex: 2,
			wantResult:           []string{"0", "2", "4", "6", "8"},
		},
		{
			KafkaTopicSliceCount: 2,
			KafkaTopicSliceIndex: 3,
			wantResult:           []string{"1", "3", "5", "7", "9"},
		},
		{
			KafkaTopicSliceCount: 3,
			KafkaTopicSliceIndex: 0,
			wantResult:           []string{"0", "3", "6", "9"},
		},
		{
			KafkaTopicSliceCount: 3,
			KafkaTopicSliceIndex: 1,
			wantResult:           []string{"1", "4", "7"},
		},
		{
			KafkaTopicSliceCount: 3,
			KafkaTopicSliceIndex: 2,
			wantResult:           []string{"2", "5", "8"},
		},
		{
			KafkaTopicSliceCount: 3,
			KafkaTopicSliceIndex: 3,
			wantResult:           []string{"0", "3", "6", "9"},
		},
		{
			KafkaTopicSliceCount: 3,
			KafkaTopicSliceIndex: 4,
			wantResult:           []string{"1", "4", "7"},
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v of %v", tt.KafkaTopicSliceIndex, tt.KafkaTopicSliceCount), func(t *testing.T) {
			if gotResult := sliceTopics(configuration.Config{KafkaTopicSliceIndex: tt.KafkaTopicSliceIndex, KafkaTopicSliceCount: tt.KafkaTopicSliceCount}, topics); !reflect.DeepEqual(gotResult, tt.wantResult) {
				t.Errorf("sliceTopics() = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}
