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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"github.com/SENERGY-Platform/models/go/models"
	"io"
	"log"
	"net/http"
	"net/url"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

type Payload = string

func New(ctx context.Context, wg *sync.WaitGroup, config configuration.Config) (result *Impl, err error) {
	result = &Impl{config: config}
	result.protocol, err = jsonCast[models.Protocol](config.FogProtocol)
	return result, err
}

type Impl struct {
	config   configuration.Config
	protocol models.Protocol
}

func (this *Impl) IsImportMessage(message model.ConsumerMessage) bool {
	return false
}

func (this *Impl) ParseImportMessage(message model.ConsumerMessage) (importId string, payload Payload, err error) {
	err = errors.New("import messages not supported in fog mode")
	return
}

func (this *Impl) GetImportEventDescriptions(importId string) ([]model.EventDesc, error) {
	return nil, errors.New("import messages not supported in fog mode")
}

func (this *Impl) IsServiceMessage(message model.ConsumerMessage) bool {
	if strings.HasPrefix(message.Topic, "event/") {
		return true
	}
	return false
}

func (this *Impl) ParseServiceMessage(message model.ConsumerMessage) (deviceId string, serviceId string, payload Payload, err error) {
	payload = string(message.Message)
	_, deviceId, serviceId, err = ParseTopic(message.Topic)
	return
}

func (this *Impl) GetServiceEventDescriptions(deviceId string, serviceId string) (result []model.EventDesc, err error) {
	query := url.Values{
		"local_device_id":  {deviceId},
		"local_service_id": {serviceId},
	}
	req, err := http.NewRequest("GET", this.config.MgwProcessSyncClientUrl+"/event-descriptions?"+query.Encode(), nil)
	if err != nil {
		return result, err
	}
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		temp, _ := io.ReadAll(resp.Body)
		return result, fmt.Errorf("%v", strings.TrimSpace(string(temp)))
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		log.Println("ERROR:", err.Error())
		debug.PrintStack()
		return result, fmt.Errorf("%v", err.Error())
	}
	return result, nil
}

func (this *Impl) SerializeMessage(payload Payload, service models.Service) (result model.SerializedMessage, err error) {
	return this.unmarshalMsg(service, this.protocol, map[string]string{this.config.FogProtocolDataFieldName: payload})
}

func ParseTopic(topic string) (prefix string, deviceUri string, serviceUri string, err error) {
	parts := strings.Split(topic, "/")
	if len(parts) != 3 {
		err = errors.New("expect 3-part topic with prefix/device-uri/service-uri")
		return
	}
	return parts[0], parts[1], parts[2], nil
}

func (this *Impl) ResetCache() {}
