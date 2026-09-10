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

package model

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestEventDescGetAspectIds(t *testing.T) {
	t.Run("uses the deprecated aspect id as a one element list", func(t *testing.T) {
		desc := EventDesc{AspectId: "aid"}
		if !reflect.DeepEqual(desc.GetAspectIds(), []string{"aid"}) {
			t.Error(desc.GetAspectIds())
		}
	})

	t.Run("uses the list when only the list is set", func(t *testing.T) {
		desc := EventDesc{AspectIds: []string{"aid1", "aid2"}}
		if !reflect.DeepEqual(desc.GetAspectIds(), []string{"aid1", "aid2"}) {
			t.Error(desc.GetAspectIds())
		}
	})

	t.Run("appends the deprecated aspect id to a list that misses it", func(t *testing.T) {
		desc := EventDesc{AspectId: "aid1", AspectIds: []string{"aid2"}}
		if !reflect.DeepEqual(desc.GetAspectIds(), []string{"aid2", "aid1"}) {
			t.Error(desc.GetAspectIds())
		}
	})

	t.Run("does not repeat a deprecated aspect id the list already contains", func(t *testing.T) {
		desc := EventDesc{AspectId: "aid1", AspectIds: []string{"aid1", "aid2"}}
		if !reflect.DeepEqual(desc.GetAspectIds(), []string{"aid1", "aid2"}) {
			t.Error(desc.GetAspectIds())
		}
	})

	t.Run("returns nothing when no aspect is set", func(t *testing.T) {
		desc := EventDesc{}
		if len(desc.GetAspectIds()) != 0 {
			t.Error(desc.GetAspectIds())
		}
	})

}

// TestEventDescAspectIdsJson covers the boundary the descriptions actually arrive over:
// they are written by another service and reach the worker as json, in fog mode over http
// and in cloud mode out of mongo. A description stored before the aspect lists carries
// aspect_id alone, so both spellings have to answer with the same aspects.
func TestEventDescAspectIdsJson(t *testing.T) {
	t.Run("reads a description that carries only the deprecated aspect id", func(t *testing.T) {
		desc := EventDesc{}
		err := json.Unmarshal([]byte(`{"aspect_id":"aid"}`), &desc)
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(desc.GetAspectIds(), []string{"aid"}) {
			t.Error(desc.GetAspectIds())
		}
	})

	t.Run("reads a description that carries an aspect list", func(t *testing.T) {
		desc := EventDesc{}
		err := json.Unmarshal([]byte(`{"aspect_ids":["aid1","aid2"]}`), &desc)
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(desc.GetAspectIds(), []string{"aid1", "aid2"}) {
			t.Error(desc.GetAspectIds())
		}
	})

	t.Run("omits an unset aspect list, so that an old reader sees the payload it knows", func(t *testing.T) {
		temp, err := json.Marshal(EventDesc{AspectId: "aid"})
		if err != nil {
			t.Error(err)
			return
		}
		if strings.Contains(string(temp), "aspect_ids") {
			t.Error(string(temp))
		}
	})
}
