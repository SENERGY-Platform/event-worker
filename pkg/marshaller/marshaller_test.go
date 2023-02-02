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

package marshaller

import (
	"context"
	"errors"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"github.com/SENERGY-Platform/models/go/models"
	"reflect"
	"sync"
	"testing"
)

func TestMarshaller(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	config.Debug = true
	m, err := New(ctx, wg, config, DeviceRepoMock{
		GetCharacteristicF: func(id string) (characteristic models.Characteristic, err error) {
			characteristic, ok := map[string]models.Characteristic{
				"devicevalue": {
					Id:   "devicevalue",
					Name: "devicevalue",
					Type: models.Integer,
				},
				"requestvalue": {
					Id:   "requestvalue",
					Name: "requestvalue",
					Type: models.Integer,
				},
			}[id]
			if !ok {
				return characteristic, errors.New("not found")
			}
			return characteristic, nil
		},
		GetConceptF: func(id string) (concept models.Concept, err error) {
			concept, ok := map[string]models.Concept{
				"testconcept": {
					Id:                   "testconcept",
					Name:                 "testconcept",
					CharacteristicIds:    []string{"devicevalue", "requestvalue"},
					BaseCharacteristicId: "requestvalue",
					Conversions: []models.ConverterExtension{
						{
							From:            "requestvalue",
							To:              "devicevalue",
							Distance:        0,
							Formula:         "x - 10",
							PlaceholderName: "x",
						},
						{
							From:            "devicevalue",
							To:              "requestvalue",
							Distance:        0,
							Formula:         "x + 10",
							PlaceholderName: "x",
						},
					},
				},
			}[id]
			if !ok {
				return concept, errors.New("not found")
			}
			return concept, nil
		},
		GetConceptIdOfFunctionF: func(id string) string {
			return map[string]string{
				"fid": "testconcept",
			}[id]
		},
		GetAspectNodeF: func(id string) (models.AspectNode, error) {
			result, ok := map[string]models.AspectNode{
				"aid_parent": {
					Id:            "aid_parent",
					Name:          "aid_parent",
					RootId:        "aid_parent",
					ChildIds:      []string{"aid"},
					AncestorIds:   []string{},
					DescendentIds: []string{"aid", "aid_child"},
				},
				"aid": {
					Id:            "aid",
					Name:          "aid",
					RootId:        "aid_parent",
					ParentId:      "aid_parent",
					ChildIds:      []string{"aid_child"},
					AncestorIds:   []string{"aid_parent"},
					DescendentIds: []string{"aid_child"},
				},
				"aid_child": {
					Id:            "aid_child",
					Name:          "aid_child",
					RootId:        "aid_parent",
					ParentId:      "aid",
					ChildIds:      []string{},
					AncestorIds:   []string{"aid_parent", "aid"},
					DescendentIds: []string{},
				},
			}[id]
			if !ok {
				return result, errors.New("not found")
			}
			return result, nil
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	t.Run("without path", func(t *testing.T) {
		result, err := m.Unmarshal(model.EventMessageDesc{
			EventDesc: model.EventDesc{
				CharacteristicId: "requestvalue",
				FunctionId:       "fid",
				AspectId:         "aid",
				ServiceForMarshaller: models.Service{
					Id:          "sid",
					LocalId:     "lsid",
					Interaction: models.EVENT,
					ProtocolId:  "pid",
					Outputs: []models.Content{
						{
							Id: "output",
							ContentVariable: models.ContentVariable{
								Id:   "outputcontentid",
								Name: "outputcontent",
								Type: models.Structure,
								SubContentVariables: []models.ContentVariable{
									{
										Id:               "value",
										Name:             "value",
										Type:             models.Integer,
										CharacteristicId: "devicevalue",
										FunctionId:       "fid",
										AspectId:         "aid",
									},
									{
										Id:   "time",
										Name: "time",
										Type: models.Integer,
									},
								},
							},
							Serialization:     models.JSON,
							ProtocolSegmentId: "output",
						},
					},
				},
			},
			Message: map[string]interface{}{
				"outputcontent": map[string]interface{}{
					"time":  3333,
					"value": 42,
				},
			},
		})
		if err != nil {
			t.Error(err)
			return
		}
		var expectedResult interface{}
		expectedResult = float64(52)
		if !reflect.DeepEqual(result, expectedResult) {
			t.Errorf("\n%#v\n%#v", result, expectedResult)
			return
		}
	})
	t.Run("with path", func(t *testing.T) {
		result, err := m.Unmarshal(model.EventMessageDesc{
			EventDesc: model.EventDesc{
				CharacteristicId: "requestvalue",
				FunctionId:       "fid",
				AspectId:         "aid",
				Path:             "outputcontent.value",
				ServiceForMarshaller: models.Service{
					Id:          "sid",
					LocalId:     "lsid",
					Interaction: models.EVENT,
					ProtocolId:  "pid",
					Outputs: []models.Content{
						{
							Id: "output",
							ContentVariable: models.ContentVariable{
								Id:   "outputcontentid",
								Name: "outputcontent",
								Type: models.Structure,
								SubContentVariables: []models.ContentVariable{
									{
										Id:               "value",
										Name:             "value",
										Type:             models.Integer,
										CharacteristicId: "devicevalue",
										FunctionId:       "fid",
										AspectId:         "aid",
									},
									{
										Id:   "time",
										Name: "time",
										Type: models.Integer,
									},
								},
							},
							Serialization:     models.JSON,
							ProtocolSegmentId: "output",
						},
					},
				},
			},
			Message: map[string]interface{}{
				"outputcontent": map[string]interface{}{
					"time":  3333,
					"value": 42,
				},
			},
		})
		if err != nil {
			t.Error(err)
			return
		}
		var expectedResult interface{}
		expectedResult = float64(52)
		if !reflect.DeepEqual(result, expectedResult) {
			t.Errorf("\n%#v\n%#v", result, expectedResult)
			return
		}
	})
	t.Run("device uses child aspect", func(t *testing.T) {
		result, err := m.Unmarshal(model.EventMessageDesc{
			EventDesc: model.EventDesc{
				CharacteristicId: "requestvalue",
				FunctionId:       "fid",
				AspectId:         "aid",
				ServiceForMarshaller: models.Service{
					Id:          "sid",
					LocalId:     "lsid",
					Interaction: models.EVENT,
					ProtocolId:  "pid",
					Outputs: []models.Content{
						{
							Id: "output",
							ContentVariable: models.ContentVariable{
								Id:   "outputcontentid",
								Name: "outputcontent",
								Type: models.Structure,
								SubContentVariables: []models.ContentVariable{
									{
										Id:               "value",
										Name:             "value",
										Type:             models.Integer,
										CharacteristicId: "devicevalue",
										FunctionId:       "fid",
										AspectId:         "aid_child",
									},
									{
										Id:   "time",
										Name: "time",
										Type: models.Integer,
									},
								},
							},
							Serialization:     models.JSON,
							ProtocolSegmentId: "output",
						},
					},
				},
			},
			Message: map[string]interface{}{
				"outputcontent": map[string]interface{}{
					"time":  3333,
					"value": 42,
				},
			},
		})
		if err != nil {
			t.Error(err)
			return
		}
		var expectedResult interface{}
		expectedResult = float64(52)
		if !reflect.DeepEqual(result, expectedResult) {
			t.Errorf("\n%#v\n%#v", result, expectedResult)
			return
		}
	})
}

func ptr(s string) *string {
	return &s
}

type DeviceRepoMock struct {
	GetCharacteristicF      func(id string) (characteristic models.Characteristic, err error)
	GetConceptF             func(id string) (concept models.Concept, err error)
	GetConceptIdOfFunctionF func(id string) string
	GetAspectNodeF          func(id string) (models.AspectNode, error)
}

func (this DeviceRepoMock) GetCharacteristic(id string) (characteristic models.Characteristic, err error) {
	return this.GetCharacteristicF(id)
}

func (this DeviceRepoMock) GetConcept(id string) (concept models.Concept, err error) {
	return this.GetConceptF(id)
}

func (this DeviceRepoMock) GetConceptIdOfFunction(id string) string {
	return this.GetConceptIdOfFunctionF(id)
}

func (this DeviceRepoMock) GetAspectNode(id string) (models.AspectNode, error) {
	return this.GetAspectNodeF(id)
}
