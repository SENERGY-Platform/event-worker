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
	"encoding/json"
	"fmt"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"github.com/dop251/goja"
	"strconv"
	"time"
)

func (this *Worker) evaluateScript(desc model.EventMessageDesc, value interface{}) (trigger bool, err error) {
	if this.config.Mode == configuration.FogMode && this.config.Debug {
		defer func() {
			variablesJson, _ := json.Marshal(desc.Variables)
			descInfoJson, _ := json.Marshal(map[string]string{
				"device":     desc.DeviceId,
				"service":    desc.ServiceId,
				"deployment": desc.DeploymentId,
			})
			fmt.Printf("evaluate script for %v: f(%v, %v){%v}; f(%#v, %v) = %v; error = %v\n", string(descInfoJson), desc.ValueVariable, string(variablesJson), desc.Script, value, string(variablesJson), trigger, err)
		}()
	} else if desc.DeviceId != "" {
		defer func() {
			variablesJson, _ := json.Marshal(desc.Variables)
			this.config.LogTrace(desc.DeviceId, fmt.Sprintf("evaluate script: f(%v, %v){%v}; f(%#v, %v) = %v; error = %v\n", desc.ValueVariable, string(variablesJson), desc.Script, value, string(variablesJson), trigger, err))
		}()
	}
	vm := goja.New()
	time.AfterFunc(2*time.Second, func() {
		vm.Interrupt("script execution timeout")
	})
	vm.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))
	err = vm.Set(desc.ValueVariable, value)
	if err != nil {
		return false, fmt.Errorf("%w: %v", model.MessageIgnoreError, err.Error())
	}
	for key, variableStr := range desc.Variables {
		var variable interface{}
		err := json.Unmarshal([]byte(variableStr), &variable)
		if err != nil {
			variable = variableStr
		}
		err = vm.Set(key, variable)
		if err != nil {
			return false, fmt.Errorf("%w: %v", model.MessageIgnoreError, err.Error())
		}
	}
	result, err := vm.RunString(desc.Script)
	if err != nil {
		return false, fmt.Errorf("%w: %v", model.MessageIgnoreError, err.Error())
	}
	resultInterface := result.Export()
	switch v := resultInterface.(type) {
	case bool:
		return v, nil
	case string:
		trigger, err = strconv.ParseBool(v)
		if err != nil {
			return false, fmt.Errorf("%w: %v", model.MessageIgnoreError, err.Error())
		}
		return trigger, nil
	case int:
		return v >= 1, nil
	case int32:
		return v >= 1, nil
	case int64:
		return v >= 1, nil
	case float32:
		return v >= 1, nil
	case float64:
		return v >= 1, nil
	default:
		return false, fmt.Errorf("%w: unknown script result type for %v", model.MessageIgnoreError, v)
	}
}
