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

package eventrepo

import (
	"context"
	"encoding/json"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"github.com/SENERGY-Platform/models/go/models"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
)

func TestFogEventRepo(t *testing.T) {
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

	deviceDesc := model.EventDesc{
		UserId:           "testuser",
		DeploymentId:     "deplid1",
		DeviceId:         "local_device",
		ServiceId:        "local_service",
		Script:           "x == 42",
		ValueVariable:    "x",
		EventId:          "event-id",
		CharacteristicId: "cid",
		FunctionId:       "fid",
		AspectId:         "aid",
		ServiceForMarshaller: models.Service{
			Id:          "service",
			LocalId:     "lservice",
			Name:        "service",
			Interaction: models.EVENT,
			ProtocolId:  "pid",
			Outputs: []models.Content{
				{
					Id: "output",
					ContentVariable: models.ContentVariable{
						Id:               "cv",
						Name:             "value",
						Type:             models.Integer,
						CharacteristicId: "cid",
						FunctionId:       "fid",
						AspectId:         "aid",
					},
					Serialization:     "json",
					ProtocolSegmentId: "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65",
				},
			},
		},
	}

	s := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		result := []model.EventDesc{}
		if request.URL.Query().Get("local_device_id") == deviceDesc.DeviceId &&
			request.URL.Query().Get("local_service_id") == deviceDesc.ServiceId {
			result = append(result, deviceDesc)
		}
		json.NewEncoder(writer).Encode(result)
	}))

	defer s.Close()
	config.MgwProcessSyncClientUrl = s.URL

	repo, err := New(ctx, wg, config, nil)
	if err != nil {
		t.Error(err)
		return
	}

	t.Run("device/service", func(t *testing.T) {
		desc, err := repo.Get(model.ConsumerMessage{
			Topic:   "event/local_device/local_service",
			Message: []byte(`42`),
		})
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(desc, []model.EventMessageDesc{{
			EventDesc: deviceDesc,
			Message:   map[string]interface{}{"value": float64(42)},
		}}) {
			t.Errorf("\n%#v\n%#v", deviceDesc, desc)
		}
	})

	t.Run("unhandled device/service", func(t *testing.T) {
		desc, err := repo.Get(model.ConsumerMessage{
			Topic:   "event/local_device_2/local_service_2",
			Message: []byte(`42`),
		})
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(desc, []model.EventMessageDesc{}) {
			t.Errorf("\n%#v\n%#v", deviceDesc, desc)
		}
	})

	t.Run("unknown topic type", func(t *testing.T) {
		desc, err := repo.Get(model.ConsumerMessage{
			Topic:   "foo",
			Message: []byte(`42`),
		})
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(desc, []model.EventMessageDesc{}) {
			t.Errorf("\n%#v\n%#v", deviceDesc, desc)
		}
	})
}
