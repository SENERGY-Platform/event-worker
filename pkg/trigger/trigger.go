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
	"fmt"
	"github.com/SENERGY-Platform/event-worker/pkg/auth"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"io"
	"net/http"
	"runtime/debug"
	"strings"
	"time"
)

func New(config configuration.Config, auth *auth.Auth) (*Trigger, error) {
	return &Trigger{
		config: config,
		auth:   auth,
	}, nil
}

type Trigger struct {
	config configuration.Config
	auth   *auth.Auth
}

func (this *Trigger) Trigger(desc model.EventMessageDesc, value interface{}) error {
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
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%w: %v", model.MessageIgnoreError, strings.TrimSpace(string(response)))
	}
	return err
}

type CamundaVariable struct {
	Type  string      `json:"type,omitempty"`
	Value interface{} `json:"value,omitempty"`
}
