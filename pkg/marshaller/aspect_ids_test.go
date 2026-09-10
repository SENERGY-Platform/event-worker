/*
 * Copyright (c) 2026 InfAI (CC SES)
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
	"strings"
	"sync"
	"testing"

	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"github.com/SENERGY-Platform/models/go/models"
)

// TestUnmarshalWithAspectIds covers the path selection of a description that asks for
// several aspects. Every requested aspect has to be carried by the same content variable,
// each of them covering its own aspect subtree downwards, so the aspects of a description
// select one output path out of several that share the function.
//
// The fixtures distinguish the paths by their value: the message carries a different
// number per path and the characteristic conversion adds ten, so the returned value names
// the path that was chosen. Where a wrong implementation would answer with another path,
// that path is listed first in its service, so that plain content order is not enough to
// pass the test.
func TestUnmarshalWithAspectIds(t *testing.T) {
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

	m, err := New(ctx, wg, config, aspectTestDeviceRepo())
	if err != nil {
		t.Error(err)
		return
	}

	unmarshal := func(service models.Service, desc model.EventDesc) (interface{}, error) {
		desc.CharacteristicId = "requestvalue"
		desc.FunctionId = "fid"
		desc.ServiceForMarshaller = service
		return m.Unmarshal(model.EventMessageDesc{
			EventDesc: desc,
			Message:   map[string]interface{}{"outputcontent": aspectTestMessage},
		})
	}

	t.Run("selects the path that carries every requested aspect", func(t *testing.T) {
		//'inside' is the closest match for inside-air alone and carries no electricity
		//aspect, 'kitchen' is the only path answering both
		value, err := unmarshal(aspectTestService(), model.EventDesc{AspectIds: []string{"inside_air", "electricity"}})
		if err != nil {
			t.Error(err)
			return
		}
		if value != float64(13) {
			t.Error(value)
		}
	})

	t.Run("keeps the deprecated aspect id working as a one element list", func(t *testing.T) {
		value, err := unmarshal(aspectTestService(), model.EventDesc{AspectId: "outside_air"})
		if err != nil {
			t.Error(err)
			return
		}
		if value != float64(12) {
			t.Error(value)
		}
	})

	t.Run("does not accept a path that carries only an ancestor of a requested aspect", func(t *testing.T) {
		//a requested aspect covers its own subtree downwards, never upwards: 'inside'
		//carries the parent of kitchen-air and drops out although it is listed first
		value, err := unmarshal(aspectTestService(), model.EventDesc{AspectIds: []string{"kitchen_air"}})
		if err != nil {
			t.Error(err)
			return
		}
		if value != float64(13) {
			t.Error(value)
		}
	})

	t.Run("prefers the path whose aspects sit closest to the requested ones", func(t *testing.T) {
		//both paths answer the request, 'near' one level below air and 'far' two, and the
		//farther one is listed first
		value, err := unmarshal(aspectDistanceService(), model.EventDesc{AspectIds: []string{"air", "electricity"}})
		if err != nil {
			t.Error(err)
			return
		}
		if value != float64(15) {
			t.Error(value)
		}
	})

	t.Run("finds no path for two sibling aspects", func(t *testing.T) {
		//no content variable carries both siblings, so the request is unanswerable rather
		//than answered by one of the two
		_, err := unmarshal(aspectTestService(), model.EventDesc{AspectIds: []string{"inside_air", "outside_air"}})
		if !errors.Is(err, model.MessageIgnoreError) {
			t.Error(err)
			return
		}
		if !strings.Contains(err.Error(), "no output path found for criteria") {
			t.Error(err)
		}
	})

	t.Run("ignores a description without any aspect", func(t *testing.T) {
		_, err := unmarshal(aspectTestService(), model.EventDesc{})
		if !errors.Is(err, model.MessageIgnoreError) {
			t.Error(err)
			return
		}
		if !strings.Contains(err.Error(), "missing aspect id") {
			t.Error(err)
		}
	})

	t.Run("surfaces the error of an aspect that cannot be resolved", func(t *testing.T) {
		//the device-repository decides whether such an error may be retried, so it has to
		//arrive unchanged instead of being replaced by a path error
		_, err := unmarshal(aspectTestService(), model.EventDesc{AspectIds: []string{"inside_air", "unknown"}})
		if err == nil {
			t.Error("expected an error")
			return
		}
		if !strings.Contains(err.Error(), "unknown aspect node") {
			t.Error(err)
		}
	})
}

// aspectTestMessage carries a value for every output name both test services use.
var aspectTestMessage = map[string]interface{}{
	"inside":  1,
	"outside": 2,
	"kitchen": 3,
	"power":   4,
	"far":     3,
	"near":    5,
}

func aspectTestOutput(name string, aspectIds []string) models.ContentVariable {
	return models.ContentVariable{
		Id:               name,
		Name:             name,
		Type:             models.Integer,
		CharacteristicId: "devicevalue",
		FunctionId:       "fid",
		AspectIds:        aspectIds,
	}
}

func aspectTestServiceOf(outputs ...models.ContentVariable) models.Service {
	return models.Service{
		Id:          "sid",
		LocalId:     "lsid",
		Interaction: models.EVENT,
		ProtocolId:  "pid",
		Outputs: []models.Content{
			{
				Id: "output",
				ContentVariable: models.ContentVariable{
					Id:                  "outputcontentid",
					Name:                "outputcontent",
					Type:                models.Structure,
					SubContentVariables: outputs,
				},
				Serialization:     models.JSON,
				ProtocolSegmentId: "output",
			},
		},
	}
}

// aspectTestService describes outputs that share the function and differ only in the
// aspects they carry. 'inside' is listed first because it is the path a selection that
// evaluates one aspect instead of the whole list would answer with.
func aspectTestService() models.Service {
	return aspectTestServiceOf(
		aspectTestOutput("inside", []string{"inside_air"}),
		aspectTestOutput("outside", []string{"outside_air"}),
		aspectTestOutput("kitchen", []string{"kitchen_air", "electricity"}),
		aspectTestOutput("power", []string{"electricity"}),
	)
}

// aspectDistanceService describes two outputs that both answer a request over air and
// electricity, at different distances in the air hierarchy, the farther one first.
func aspectDistanceService() models.Service {
	return aspectTestServiceOf(
		aspectTestOutput("far", []string{"kitchen_air", "electricity"}),
		aspectTestOutput("near", []string{"inside_air", "electricity"}),
	)
}

// aspectTestDeviceRepo serves two aspect hierarchies: air with an inside branch that goes
// one level deeper, an outside branch as its sibling, and electricity as an unrelated root.
func aspectTestDeviceRepo() DeviceRepoMock {
	aspectNodes := map[string]models.AspectNode{
		"air": {
			Id:            "air",
			Name:          "air",
			RootId:        "air",
			ChildIds:      []string{"inside_air", "outside_air"},
			AncestorIds:   []string{},
			DescendentIds: []string{"inside_air", "outside_air", "kitchen_air"},
		},
		"inside_air": {
			Id:            "inside_air",
			Name:          "inside_air",
			RootId:        "air",
			ParentId:      "air",
			ChildIds:      []string{"kitchen_air"},
			AncestorIds:   []string{"air"},
			DescendentIds: []string{"kitchen_air"},
		},
		"outside_air": {
			Id:            "outside_air",
			Name:          "outside_air",
			RootId:        "air",
			ParentId:      "air",
			ChildIds:      []string{},
			AncestorIds:   []string{"air"},
			DescendentIds: []string{},
		},
		"kitchen_air": {
			Id:            "kitchen_air",
			Name:          "kitchen_air",
			RootId:        "air",
			ParentId:      "inside_air",
			ChildIds:      []string{},
			AncestorIds:   []string{"air", "inside_air"},
			DescendentIds: []string{},
		},
		"electricity": {
			Id:            "electricity",
			Name:          "electricity",
			RootId:        "electricity",
			ChildIds:      []string{},
			AncestorIds:   []string{},
			DescendentIds: []string{},
		},
	}
	return DeviceRepoMock{
		GetAspectNodeF: func(id string) (models.AspectNode, error) {
			result, ok := aspectNodes[id]
			if !ok {
				return result, errors.New("unknown aspect node: " + id)
			}
			return result, nil
		},
		GetCharacteristicF: func(id string) (characteristic models.Characteristic, err error) {
			characteristic, ok := map[string]models.Characteristic{
				"devicevalue":  {Id: "devicevalue", Name: "devicevalue", Type: models.Integer},
				"requestvalue": {Id: "requestvalue", Name: "requestvalue", Type: models.Integer},
			}[id]
			if !ok {
				return characteristic, errors.New("unknown characteristic: " + id)
			}
			return characteristic, nil
		},
		GetConceptIdOfFunctionF: func(id string) string {
			return map[string]string{"fid": "testconcept"}[id]
		},
		GetConceptF: func(id string) (concept models.Concept, err error) {
			concept, ok := map[string]models.Concept{
				"testconcept": {
					Id:                   "testconcept",
					Name:                 "testconcept",
					CharacteristicIds:    []string{"devicevalue", "requestvalue"},
					BaseCharacteristicId: "requestvalue",
					Conversions: []models.ConverterExtension{
						{From: "requestvalue", To: "devicevalue", Formula: "x - 10", PlaceholderName: "x"},
						{From: "devicevalue", To: "requestvalue", Formula: "x + 10", PlaceholderName: "x"},
					},
				},
			}[id]
			if !ok {
				return concept, errors.New("unknown concept: " + id)
			}
			return concept, nil
		},
	}
}
