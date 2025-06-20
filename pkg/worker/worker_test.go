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

package worker

import (
	"context"
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestWorkerSimpleScriptQos0(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}

	message := "testmessage"
	testtopic := "testtopic"

	triggeredMsg := map[string][]interface{}{}
	triggerMux := sync.Mutex{}

	w, err := New(ctx,
		wg,
		config,
		MockEventRepo{F: func(topic string) (eventDesc []model.EventDesc, err error) {
			if topic != testtopic {
				t.Error("unexpected topic", topic)
				return nil, errors.New("unexpected topic")
			}
			return []model.EventDesc{
				{
					Script:        `value == "marshalled:testmessage"`,
					ValueVariable: "value",
					Variables:     nil,
					Qos:           0,
					EventId:       "eventid_1",
					UserId:        "user",
				},
				{
					Script:        `value == "marshalled:testmessage"`,
					ValueVariable: "value",
					Variables:     nil,
					Qos:           0,
					EventId:       "eventid_2",
					UserId:        "user",
				},
				{
					Script:        `value == "marshalled:nope"`,
					ValueVariable: "value",
					Variables:     nil,
					Qos:           0,
					EventId:       "eventid_3",
					UserId:        "user",
				},
			}, nil
		}},
		MockMarshaller{F: func(desc model.EventMessageDesc) (value interface{}, err error) {
			msg, ok := desc.Message["body"].(string)
			if !ok {
				t.Errorf("unexpected message: %#v", desc.Message)
				return nil, errors.New("unexpected message")
			}
			if msg != message {
				t.Error("unexpected message:", msg)
				return nil, errors.New("unexpected message")
			}
			return fmt.Sprintf("marshalled:%v", msg), nil
		}},
		MockTrigger{F: func(desc model.EventMessageDesc, value interface{}) error {
			triggerMux.Lock()
			defer triggerMux.Unlock()
			triggeredMsg[desc.EventId] = append(triggeredMsg[desc.EventId], value)
			return nil
		}},
		MockNotifier{F: func(desc model.EventMessageDesc, err error) {
			t.Error(err)
		}},
	)
	if err != nil {
		t.Error(err)
		return
	}
	err = w.Do(model.ConsumerMessage{testtopic, []byte(message), 0, ""})
	if err != nil {
		t.Error(err)
		return
	}
	err = w.Do(model.ConsumerMessage{testtopic, []byte(message), 0, ""})
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(100 * time.Millisecond)
	triggerMux.Lock()
	defer triggerMux.Unlock()
	if !reflect.DeepEqual(triggeredMsg, map[string][]interface{}{
		"eventid_1": {"marshalled:testmessage", "marshalled:testmessage"},
		"eventid_2": {"marshalled:testmessage", "marshalled:testmessage"},
	}) {
		t.Errorf("%#v", triggeredMsg)
	}
}

func TestWorkerSimpleScriptQos1(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}

	message := "testmessage"
	testtopic := "testtopic"

	triggeredMsg := map[string][]interface{}{}
	triggerMux := sync.Mutex{}

	w, err := New(ctx,
		wg,
		config,
		MockEventRepo{F: func(topic string) (eventDesc []model.EventDesc, err error) {
			if topic != testtopic {
				t.Error("unexpected topic", topic)
				return nil, errors.New("unexpected topic")
			}
			return []model.EventDesc{
				{
					Script:        `value == "marshalled:testmessage"`,
					ValueVariable: "value",
					Variables:     nil,
					Qos:           1,
					EventId:       "eventid_1",
					UserId:        "user",
				},
				{
					Script:        `value == "marshalled:testmessage"`,
					ValueVariable: "value",
					Variables:     nil,
					Qos:           1,
					EventId:       "eventid_2",
					UserId:        "user",
				},
				{
					Script:        `value == "marshalled:nope"`,
					ValueVariable: "value",
					Variables:     nil,
					Qos:           1,
					EventId:       "eventid_3",
					UserId:        "user",
				},
			}, nil
		}},
		MockMarshaller{F: func(desc model.EventMessageDesc) (value interface{}, err error) {
			msg, ok := desc.Message["body"].(string)
			if !ok {
				t.Errorf("unexpected message: %#v", desc.Message)
				return nil, errors.New("unexpected message")
			}
			if msg != message {
				t.Error("unexpected message:", msg)
				return nil, errors.New("unexpected message")
			}
			return fmt.Sprintf("marshalled:%v", msg), nil
		}},
		MockTrigger{F: func(desc model.EventMessageDesc, value interface{}) error {
			triggerMux.Lock()
			defer triggerMux.Unlock()
			triggeredMsg[desc.EventId] = append(triggeredMsg[desc.EventId], value)
			return nil
		}},
		MockNotifier{F: func(desc model.EventMessageDesc, err error) {
			t.Error(err)
		}},
	)
	if err != nil {
		t.Error(err)
		return
	}
	err = w.Do(model.ConsumerMessage{testtopic, []byte(message), 0, ""})
	if err != nil {
		t.Error(err)
		return
	}
	err = w.Do(model.ConsumerMessage{testtopic, []byte(message), 0, ""})
	if err != nil {
		t.Error(err)
		return
	}
	triggerMux.Lock()
	defer triggerMux.Unlock()
	if !reflect.DeepEqual(triggeredMsg, map[string][]interface{}{
		"eventid_1": {"marshalled:testmessage", "marshalled:testmessage"},
		"eventid_2": {"marshalled:testmessage", "marshalled:testmessage"},
	}) {
		t.Errorf("%#v", triggeredMsg)
	}
}

func TestWorkerSimpleScriptQosMixed(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}

	message := "testmessage"
	testtopic := "testtopic"

	triggeredMsg := map[string][]interface{}{}
	triggerMux := sync.Mutex{}

	w, err := New(ctx,
		wg,
		config,
		MockEventRepo{F: func(topic string) (eventDesc []model.EventDesc, err error) {
			if topic != testtopic {
				t.Error("unexpected topic", topic)
				return nil, errors.New("unexpected topic")
			}
			return []model.EventDesc{
				{
					Script:        `value == "marshalled:testmessage"`,
					ValueVariable: "value",
					Variables:     nil,
					Qos:           1,
					EventId:       "eventid_1",
					UserId:        "user",
				},
				{
					Script:        `value == "marshalled:testmessage"`,
					ValueVariable: "value",
					Variables:     nil,
					Qos:           0,
					EventId:       "eventid_2",
					UserId:        "user",
				},
				{
					Script:        `value == "marshalled:nope"`,
					ValueVariable: "value",
					Variables:     nil,
					Qos:           1,
					EventId:       "eventid_3",
					UserId:        "user",
				},
				{
					Script:        `value == "marshalled:nope"`,
					ValueVariable: "value",
					Variables:     nil,
					Qos:           0,
					EventId:       "eventid_4",
					UserId:        "user",
				},
			}, nil
		}},
		MockMarshaller{F: func(desc model.EventMessageDesc) (value interface{}, err error) {
			msg, ok := desc.Message["body"].(string)
			if !ok {
				t.Errorf("unexpected message: %#v", desc.Message)
				return nil, errors.New("unexpected message")
			}
			if msg != message {
				t.Error("unexpected message:", msg)
				return nil, errors.New("unexpected message")
			}
			return fmt.Sprintf("marshalled:%v", msg), nil
		}},
		MockTrigger{F: func(desc model.EventMessageDesc, value interface{}) error {
			triggerMux.Lock()
			defer triggerMux.Unlock()
			triggeredMsg[desc.EventId] = append(triggeredMsg[desc.EventId], value)
			return nil
		}},
		MockNotifier{F: func(desc model.EventMessageDesc, err error) {
			t.Error(err)
		}},
	)
	if err != nil {
		t.Error(err)
		return
	}
	err = w.Do(model.ConsumerMessage{testtopic, []byte(message), 0, ""})
	if err != nil {
		t.Error(err)
		return
	}
	err = w.Do(model.ConsumerMessage{testtopic, []byte(message), 0, ""})
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(100 * time.Millisecond)
	triggerMux.Lock()
	defer triggerMux.Unlock()
	if !reflect.DeepEqual(triggeredMsg, map[string][]interface{}{
		"eventid_1": {"marshalled:testmessage", "marshalled:testmessage"},
		"eventid_2": {"marshalled:testmessage", "marshalled:testmessage"},
	}) {
		t.Errorf("%#v", triggeredMsg)
	}
}

func TestWorkerComplexScriptQos1(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}

	message := "testmessage"
	testtopic := "testtopic"

	triggeredMsg := map[string][]interface{}{}
	triggerMux := sync.Mutex{}

	w, err := New(ctx,
		wg,
		config,
		MockEventRepo{F: func(topic string) (eventDesc []model.EventDesc, err error) {
			if topic != testtopic {
				t.Error("unexpected topic", topic)
				return nil, errors.New("unexpected topic")
			}
			return []model.EventDesc{
				{
					Script:        `value == expected`,
					ValueVariable: "value",
					Variables:     map[string]string{"expected": "marshalled:testmessage"},
					Qos:           1,
					EventId:       "eventid_1",
					UserId:        "user",
				},
				{
					Script:        `value == expected`,
					ValueVariable: "value",
					Variables:     map[string]string{"expected": "marshalled:testmessage"},
					Qos:           1,
					EventId:       "eventid_2",
					UserId:        "user",
				},
				{
					Script:        `value == expected`,
					ValueVariable: "value",
					Variables:     map[string]string{"expected": "marshalled:nope"},
					Qos:           1,
					EventId:       "eventid_3",
					UserId:        "user",
				},
			}, nil
		}},
		MockMarshaller{F: func(desc model.EventMessageDesc) (value interface{}, err error) {
			msg, ok := desc.Message["body"].(string)
			if !ok {
				t.Errorf("unexpected message: %#v", desc.Message)
				return nil, errors.New("unexpected message")
			}
			if msg != message {
				t.Error("unexpected message:", msg)
				return nil, errors.New("unexpected message")
			}
			return fmt.Sprintf("marshalled:%v", msg), nil
		}},
		MockTrigger{F: func(desc model.EventMessageDesc, value interface{}) error {
			triggerMux.Lock()
			defer triggerMux.Unlock()
			triggeredMsg[desc.EventId] = append(triggeredMsg[desc.EventId], value)
			return nil
		}},
		MockNotifier{F: func(desc model.EventMessageDesc, err error) {
			t.Error(err)
		}},
	)
	if err != nil {
		t.Error(err)
		return
	}
	err = w.Do(model.ConsumerMessage{testtopic, []byte(message), 0, ""})
	if err != nil {
		t.Error(err)
		return
	}
	err = w.Do(model.ConsumerMessage{testtopic, []byte(message), 0, ""})
	if err != nil {
		t.Error(err)
		return
	}
	triggerMux.Lock()
	defer triggerMux.Unlock()
	if !reflect.DeepEqual(triggeredMsg, map[string][]interface{}{
		"eventid_1": {"marshalled:testmessage", "marshalled:testmessage"},
		"eventid_2": {"marshalled:testmessage", "marshalled:testmessage"},
	}) {
		t.Errorf("%#v", triggeredMsg)
	}
}

type MockTrigger struct {
	F   func(desc model.EventMessageDesc, value interface{}) error
	mux sync.Mutex
}

func (this MockTrigger) Trigger(desc model.EventMessageDesc, value interface{}) error {
	this.mux.Lock()
	defer this.mux.Unlock()
	return this.F(desc, value)
}

type MockNotifier struct {
	F   func(desc model.EventMessageDesc, err error)
	mux sync.Mutex
}

func (this MockNotifier) NotifyError(desc model.EventMessageDesc, err error) error {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.F(desc, err)
	return nil
}

type MockMarshaller struct {
	F   func(desc model.EventMessageDesc) (value interface{}, err error)
	mux sync.Mutex
}

func (this MockMarshaller) Unmarshal(desc model.EventMessageDesc) (value interface{}, err error) {
	this.mux.Lock()
	defer this.mux.Unlock()
	return this.F(desc)
}

type MockEventRepo struct {
	F   func(topic string) (eventDesc []model.EventDesc, err error)
	mux sync.Mutex
}

func (this MockEventRepo) ResetCache() {}

func (this MockEventRepo) Get(msg model.ConsumerMessage) (eventDesc []model.EventMessageDesc, err error) {
	this.mux.Lock()
	defer this.mux.Unlock()
	temp, err := this.F(msg.Topic)
	if err != nil {
		return eventDesc, err
	}
	for _, e := range temp {
		eventDesc = append(eventDesc, model.EventMessageDesc{
			EventDesc: e,
			Message: map[string]interface{}{
				"body": string(msg.Message),
			},
		})
	}
	return
}

func TestWorkerWithFogLogging(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	config.Mode = configuration.FogMode
	config.Debug = true

	message := "testmessage"
	testtopic := "testtopic"

	triggeredMsg := map[string][]interface{}{}
	triggerMux := sync.Mutex{}

	w, err := New(ctx,
		wg,
		config,
		MockEventRepo{F: func(topic string) (eventDesc []model.EventDesc, err error) {
			if topic != testtopic {
				t.Error("unexpected topic", topic)
				return nil, errors.New("unexpected topic")
			}
			return []model.EventDesc{
				{
					Script:        `value == "marshalled:testmessage"`,
					ValueVariable: "value",
					Variables:     nil,
					Qos:           0,
					EventId:       "eventid_1",
					UserId:        "user",
				},
				{
					Script:        `value == "marshalled:testmessage"`,
					ValueVariable: "value",
					Variables:     nil,
					Qos:           0,
					EventId:       "eventid_2",
					UserId:        "user",
				},
				{
					Script:        `value == "marshalled:nope"`,
					ValueVariable: "value",
					Variables:     nil,
					Qos:           0,
					EventId:       "eventid_3",
					UserId:        "user",
				},
			}, nil
		}},
		MockMarshaller{F: func(desc model.EventMessageDesc) (value interface{}, err error) {
			msg, ok := desc.Message["body"].(string)
			if !ok {
				t.Errorf("unexpected message: %#v", desc.Message)
				return nil, errors.New("unexpected message")
			}
			if msg != message {
				t.Error("unexpected message:", msg)
				return nil, errors.New("unexpected message")
			}
			return fmt.Sprintf("marshalled:%v", msg), nil
		}},
		MockTrigger{F: func(desc model.EventMessageDesc, value interface{}) error {
			triggerMux.Lock()
			defer triggerMux.Unlock()
			triggeredMsg[desc.EventId] = append(triggeredMsg[desc.EventId], value)
			return nil
		}},
		MockNotifier{F: func(desc model.EventMessageDesc, err error) {
			t.Error(err)
		}},
	)
	if err != nil {
		t.Error(err)
		return
	}
	err = w.Do(model.ConsumerMessage{testtopic, []byte(message), 0, ""})
	if err != nil {
		t.Error(err)
		return
	}
	err = w.Do(model.ConsumerMessage{testtopic, []byte(message), 0, ""})
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(100 * time.Millisecond)
	triggerMux.Lock()
	defer triggerMux.Unlock()
	if !reflect.DeepEqual(triggeredMsg, map[string][]interface{}{
		"eventid_1": {"marshalled:testmessage", "marshalled:testmessage"},
		"eventid_2": {"marshalled:testmessage", "marshalled:testmessage"},
	}) {
		t.Errorf("%#v", triggeredMsg)
	}
}
