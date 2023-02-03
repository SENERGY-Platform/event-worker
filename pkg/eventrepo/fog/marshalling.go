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
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/marshaller/lib/marshaller/model"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/platform-connector-lib/marshalling"
	lib_model "github.com/SENERGY-Platform/platform-connector-lib/model"
	"runtime/debug"
)

var ErrUnexpectedField = errors.New("unexpected field")
var ErrMissingField = errors.New("missing field")
var ErrUnexpectedType = errors.New("unexpected type")

func (this *Impl) unmarshalMsg(service models.Service, protocol models.Protocol, msg map[string]string) (result map[string]interface{}, err error) {
	result = map[string]interface{}{}
	fallback, fallbackKnown := marshalling.Get("json")
	for _, output := range service.Outputs {
		marshaller, ok := marshalling.Get(string(output.Serialization))
		if !ok {
			return result, errors.New("unknown format " + string(output.Serialization))
		}
		for _, segment := range protocol.ProtocolSegments {
			if segment.Id == output.ProtocolSegmentId {
				segmentMsg, ok := msg[segment.Name]
				contentVariable, err := jsonCast[lib_model.ContentVariable](output.ContentVariable)
				if err != nil {
					debug.PrintStack()
					return result, err
				}
				if ok {
					out, err := marshaller.Unmarshal(segmentMsg, contentVariable)
					if err != nil && fallbackKnown {
						out, err = fallback.Unmarshal(segmentMsg, contentVariable)
					}
					if err != nil {
						return result, err
					}
					result[output.ContentVariable.Name] = out
				}
			}
		}
	}

	result, err = CleanMsg(result, service)
	if err != nil {
		return result, err
	}
	return result, err
}

func jsonCast[T any](value interface{}) (result T, err error) {
	temp, err := json.Marshal(value)
	if err != nil {
		return result, err
	}
	err = json.Unmarshal(temp, &result)
	return result, err
}

func CleanMsg(msg map[string]interface{}, service model.Service) (result map[string]interface{}, err error) {
	result, err = RemoveUnknownFields(msg, service)
	if err != nil {
		return nil, err
	}
	result, err = DefaultMissingFields(result, service)
	if err != nil {
		return nil, err
	}
	return result, nil
}
