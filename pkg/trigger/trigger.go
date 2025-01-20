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

package trigger

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/event-worker/pkg/auth"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"github.com/SENERGY-Platform/service-commons/pkg/cache"
	"io"
	"log"
	"net/http"
	"runtime/debug"
	"strings"
	"time"
)

func New(config configuration.Config, auth *auth.Auth) (*Trigger, error) {
	c, err := cache.New(cache.Config{})
	if err != nil {
		return nil, err
	}
	maxEventTriggerInterval := time.Duration(0)
	if config.MaxEventTriggerInterval != "" && config.MaxEventTriggerInterval != "-" {
		maxEventTriggerInterval, err = time.ParseDuration(config.MaxEventTriggerInterval)
		if err != nil {
			log.Println("WARNING: unable to parse max_event_trigger_interval, fall back to 1s", err)
			maxEventTriggerInterval = time.Second
		}
	}

	return &Trigger{
		config:                  config,
		auth:                    auth,
		topicmux:                TopicMutex{},
		triggerCache:            c,
		maxEventTriggerInterval: maxEventTriggerInterval,
	}, nil
}

type Trigger struct {
	config                  configuration.Config
	auth                    *auth.Auth
	topicmux                TopicMutex
	triggerCache            *cache.Cache
	maxEventTriggerInterval time.Duration
}

func (this *Trigger) Trigger(desc model.EventMessageDesc, value interface{}) (err error) {
	if this.maxEventTriggerInterval == 0 {
		return this.trigger(desc, value)
	}
	topic := desc.UserId + "+" + desc.EventId
	this.topicmux.Lock(topic)
	defer this.topicmux.Unlock(topic)
	//every event (identified by user-id + event-id) may only happen once
	//use the cache.Use method to do the complete, only if the complete is not found in cache
	//cached by half the camunda lock duration to enable retries
	_, err = cache.Use[string](this.triggerCache, topic, func() (string, error) {
		return "", this.trigger(desc, value)
	}, cache.NoValidation, this.maxEventTriggerInterval)
	return err
}

func (this *Trigger) trigger(desc model.EventMessageDesc, value interface{}) error {
	payload, err := json.Marshal(map[string]interface{}{
		"messageName":   desc.EventId,
		"all":           true,
		"resultEnabled": false,
		"tenantId":      desc.UserId,
		"processVariablesLocal": map[string]CamundaVariable{
			"event":  {Value: value},
			"age":    {Value: desc.MessageAgeSeconds},
			"msg_id": {Value: desc.MessageId},
		},
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", this.config.EventTriggerUrl, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	if this.config.Mode == configuration.CloudMode {
		token, err := this.auth.EnsureAccess(this.config)
		if err != nil {
			debug.PrintStack()
			return err
		}
		req.Header.Set("Authorization", token)
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	response, err := io.ReadAll(resp.Body)
	if err != nil {
		debug.PrintStack()
		return fmt.Errorf("%w: %v", model.MessageIgnoreError, err.Error())
	}
	if resp.StatusCode >= 500 {
		//internal service errors may be retried
		return errors.New(strings.TrimSpace(string(response)))
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%w: %v", model.MessageIgnoreError, strings.TrimSpace(string(response)))
	}
	return err
}

type CamundaVariable struct {
	Type  string      `json:"type,omitempty"`
	Value interface{} `json:"value,omitempty"`
}
