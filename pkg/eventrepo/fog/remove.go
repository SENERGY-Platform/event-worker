/*
 * Copyright 2020 InfAI (CC SES)
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
	"fmt"
	"github.com/SENERGY-Platform/models/go/models"
	"strconv"
)

func RemoveUnknownFields(msg map[string]interface{}, service models.Service) (_ map[string]interface{}, err error) {
	for key, value := range msg {
		if variable, ok := fieldInService(key, service); ok {
			msg[key], err = removeUnknownField(value, variable)
			if err != nil {
				return msg, err
			}
		} else {
			delete(msg, key)
		}
	}
	return msg, nil
}

func removeUnknownField(value interface{}, variable models.ContentVariable) (_ interface{}, err error) {
	switch v := value.(type) {
	case string:
		if variable.Type != models.String {
			return nil, fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, models.String, variable.Type)
		}
		return value, nil
	case int:
		if variable.Type != models.Integer && variable.Type != models.Float {
			return nil, fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, models.Integer, variable.Type)
		}
		return value, nil
	case int64:
		if variable.Type != models.Integer && variable.Type != models.Float {
			return nil, fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, models.Integer, variable.Type)
		}
		return value, nil
	case float64:
		if variable.Type != models.Integer && variable.Type != models.Float {
			return nil, fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, models.Float, variable.Type)
		}
		return value, nil
	case bool:
		if variable.Type != models.Boolean {
			return nil, fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, models.Boolean, variable.Type)
		}
		return value, nil
	case map[string]interface{}:
		if variable.Type != models.Structure {
			return nil, fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, models.Structure, variable.Type)
		}
		if variable.SubContentVariables[0].Name == "*" {
			for key, subValue := range v {
				v[key], err = removeUnknownField(subValue, variable.SubContentVariables[0])
				if err != nil {
					return v, err
				}
			}
		} else {
			for key, subValue := range v {
				if subVariable, ok := fieldInVariable(key, variable); ok {
					v[key], err = removeUnknownField(subValue, subVariable)
					if err != nil {
						return v, err
					}
				} else {
					delete(v, key)
				}
			}
		}
		return v, nil
	case []interface{}:
		if variable.Type != models.List {
			return nil, fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, models.List, variable.Type)
		}
		if variable.SubContentVariables[0].Name == "*" {
			for key, subValue := range v {
				v[key], err = removeUnknownField(subValue, variable.SubContentVariables[0])
				if err != nil {
					return v, err
				}
			}
		} else {
			for i, subValue := range v {
				if subVariable, ok := fieldInVariable(strconv.Itoa(i), variable); ok {
					v[i], err = removeUnknownField(subValue, subVariable)
					if err != nil {
						return v, err
					}
				} else {
					return v[:i], nil //if list element is not found -> return only known elements
				}
			}
		}
		return v, nil
	default:
		return value, nil
	}
}

func fieldInVariable(fieldName string, variable models.ContentVariable) (models.ContentVariable, bool) {
	for _, sub := range variable.SubContentVariables {
		if sub.Name == fieldName {
			return sub, true
		}
	}
	return models.ContentVariable{}, false
}

func fieldInService(fieldName string, service models.Service) (models.ContentVariable, bool) {
	for _, output := range service.Outputs {
		if output.ContentVariable.Name == fieldName {
			return output.ContentVariable, true
		}
	}
	return models.ContentVariable{}, false
}
