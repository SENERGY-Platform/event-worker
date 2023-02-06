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
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"github.com/SENERGY-Platform/event-worker/pkg/tests/docker"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/ory/dockertest/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
	"sync"
	"testing"
)

func TestCloudEventRepo(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}

	config.Mode = configuration.CloudMode

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Error(err)
		return
	}

	_, mongoIp, err := docker.Mongo(pool, ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	config.CloudEventRepoMongoUrl = "mongodb://" + mongoIp + ":27017"

	repo, err := New(ctx, wg, config, nil)
	if err != nil {
		t.Error(err)
		return
	}

	deviceDesc := model.EventDesc{
		UserId:           "testuser",
		DeploymentId:     "deplid1",
		DeviceId:         "device",
		ServiceId:        "urn:infai:ses:service:557a8519-c801-42c6-a5e0-d6d6450ec9ab",
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

	importDesc := model.EventDesc{
		UserId:           "testuser",
		DeploymentId:     "deplid2",
		ImportId:         "urn:infai:ses:import:7f2620cb-002c-fc54-0c2e-5e840b7b0263",
		Script:           `x == "foo"`,
		ValueVariable:    "x",
		EventId:          "event-id-2",
		CharacteristicId: "cid",
		FunctionId:       "fid",
		AspectId:         "aid",
		ServiceForMarshaller: models.Service{
			Id:          "urn:infai:ses:import:7f2620cb-002c-fc54-0c2e-5e840b7b0263",
			LocalId:     "limport",
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

	t.Run("create event-repo entries", func(t *testing.T) {
		reg := bson.NewRegistryBuilder().RegisterTypeMapEntry(bsontype.EmbeddedDocument, reflect.TypeOf(bson.M{})).Build() //ensure map marshalling to interface
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(config.CloudEventRepoMongoUrl), options.Client().SetRegistry(reg))
		if err != nil {
			t.Error(err)
			return
		}

		collection := client.Database(config.CloudEventRepoMongoTable).Collection(config.CloudEventRepoMongoDescCollection)

		_, err = collection.InsertMany(ctx, []interface{}{
			deviceDesc,
			importDesc,
		})
		if err != nil {
			t.Error(err)
			return
		}
	})

	t.Run("device/service", func(t *testing.T) {
		desc, err := repo.Get(model.ConsumerMessage{
			Topic: "urn_infai_ses_service_557a8519-c801-42c6-a5e0-d6d6450ec9ab",
			Message: []byte(`{
				"device_id": "device",
				"service_id": "urn:infai:ses:service:557a8519-c801-42c6-a5e0-d6d6450ec9ab",
				"value": {
					"value": 42
				}
			}`),
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

	t.Run("import", func(t *testing.T) {
		desc, err := repo.Get(model.ConsumerMessage{
			Topic: "urn_infai_ses_import_7f2620cb-002c-fc54-0c2e-5e840b7b0263",
			Message: []byte(`{
				"import_id": "urn:infai:ses:import:7f2620cb-002c-fc54-0c2e-5e840b7b0263",
				"value": {
					"value": 42
				}
			}`),
		})
		if err != nil {
			t.Error(err)
			return
		}
		expected := []model.EventMessageDesc{{
			EventDesc: importDesc,
			Message:   map[string]interface{}{"value": float64(42)},
		}}
		if !reflect.DeepEqual(desc, expected) {
			t.Errorf("\n%#v\n%#v", expected, desc)
		}
	})

	t.Run("unhandled device/service", func(t *testing.T) {
		desc, err := repo.Get(model.ConsumerMessage{
			Topic: "urn_infai_ses_service_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
			Message: []byte(`{
				"device_id": "unhandled",
				"service_id": "unhandled",
				"value": {
					"value": 42
				}
			}`),
		})
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(desc, []model.EventMessageDesc{}) {
			t.Errorf("\n%#v\n%#v", []model.EventMessageDesc{}, desc)
		}
	})

	t.Run("unhandled import", func(t *testing.T) {
		desc, err := repo.Get(model.ConsumerMessage{
			Topic: "urn_infai_ses_import_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
			Message: []byte(`{
				"import_id": "unhandled",
				"value": {
					"value": 42
				}
			}`),
		})
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(desc, []model.EventMessageDesc{}) {
			t.Errorf("\n%#v\n%#v", []model.EventMessageDesc{}, desc)
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
			t.Errorf("\n%#v\n%#v", []model.EventMessageDesc{}, desc)
		}
	})

}
