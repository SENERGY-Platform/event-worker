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

package devicerepo

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/SENERGY-Platform/event-worker/pkg/auth"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"github.com/SENERGY-Platform/models/go/models"
)

// newTestDeviceRepo runs handler as the device-repository the repo is configured against,
// so that the requests the device-repository client builds are the ones under test.
func newTestDeviceRepo(t *testing.T, handler http.Handler) *DeviceRepo {
	t.Helper()
	wg := &sync.WaitGroup{}
	t.Cleanup(wg.Wait)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Fatal(err)
	}
	config.DeviceRepoUrl = server.URL

	repo, err := New(ctx, wg, config, &auth.Auth{})
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

func TestDeviceRepoRequests(t *testing.T) {
	requests := []string{}
	tokens := []string{}
	repo := newTestDeviceRepo(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests = append(requests, request.URL.RequestURI())
		tokens = append(tokens, request.Header.Get("Authorization"))
		var response interface{}
		switch request.URL.Path {
		case "/aspect-nodes/urn:aspect:1":
			response = models.AspectNode{Id: "urn:aspect:1", Name: "aspect", ChildIds: []string{"urn:aspect:2"}}
		case "/characteristics/urn:characteristic:1":
			response = models.Characteristic{Id: "urn:characteristic:1", Name: "characteristic", Type: models.Integer}
		case "/functions/urn:function:1":
			response = models.Function{Id: "urn:function:1", Name: "function", ConceptId: "urn:concept:1"}
		case "/concepts/urn:concept:1":
			response = models.Concept{Id: "urn:concept:1", Name: "concept", CharacteristicIds: []string{"urn:characteristic:1"}}
		default:
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		err := json.NewEncoder(writer).Encode(response)
		if err != nil {
			t.Error(err)
		}
	}))

	t.Run("reads an aspect-node", func(t *testing.T) {
		node, err := repo.GetAspectNode("urn:aspect:1")
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(node.ChildIds, []string{"urn:aspect:2"}) {
			t.Error(node)
		}
	})

	t.Run("reads a characteristic", func(t *testing.T) {
		characteristic, err := repo.GetCharacteristic("urn:characteristic:1")
		if err != nil {
			t.Error(err)
			return
		}
		if characteristic.Type != models.Integer {
			t.Error(characteristic)
		}
	})

	t.Run("reads a concept with its characteristic ids and without the characteristics", func(t *testing.T) {
		//the marshaller needs the ids and the conversions of a concept, so the answer has
		//to be a models.Concept, which the device-repository serves for sub-class=false
		concept, err := repo.GetConcept("urn:concept:1")
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(concept.CharacteristicIds, []string{"urn:characteristic:1"}) {
			t.Error(concept)
		}
	})

	t.Run("reads the concept id of a function", func(t *testing.T) {
		if conceptId := repo.GetConceptIdOfFunction("urn:function:1"); conceptId != "urn:concept:1" {
			t.Error(conceptId)
		}
	})

	t.Run("asks the endpoint of the device-repository that belongs to each entity", func(t *testing.T) {
		expected := []string{
			"/aspect-nodes/urn:aspect:1",
			"/characteristics/urn:characteristic:1",
			"/concepts/urn:concept:1?sub-class=false",
			"/functions/urn:function:1",
		}
		if !reflect.DeepEqual(requests, expected) {
			t.Error(requests)
		}
	})

	t.Run("authorizes every request", func(t *testing.T) {
		for _, token := range tokens {
			if token == "" {
				t.Error(tokens)
				return
			}
		}
	})
}

func TestDeviceRepoCache(t *testing.T) {
	count := int64(0)
	repo := newTestDeviceRepo(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		atomic.AddInt64(&count, 1)
		err := json.NewEncoder(writer).Encode(models.AspectNode{Id: "urn:aspect:1", Name: "aspect"})
		if err != nil {
			t.Error(err)
		}
	}))

	for i := 0; i < 3; i++ {
		node, err := repo.GetAspectNode("urn:aspect:1")
		if err != nil {
			t.Error(err)
			return
		}
		if node.Id != "urn:aspect:1" {
			t.Error(node)
			return
		}
	}
	if atomic.LoadInt64(&count) != 1 {
		t.Error("expected one request to the device-repository, got", atomic.LoadInt64(&count))
	}
}

// TestDeviceRepoErrors covers the retry decision the worker takes from these errors: an
// ignorable error drops the message and notifies the user, every other error is handed
// back to the consumer and the message is tried again.
func TestDeviceRepoErrors(t *testing.T) {
	status := http.StatusOK
	repo := newTestDeviceRepo(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(status)
		_, err := writer.Write([]byte("no"))
		if err != nil {
			t.Error(err)
		}
	}))

	t.Run("marks a missing aspect-node as ignorable", func(t *testing.T) {
		status = http.StatusNotFound
		_, err := repo.GetAspectNode("urn:aspect:404")
		if !errors.Is(err, model.MessageIgnoreError) {
			t.Error(err)
		}
	})

	t.Run("marks a rejected request as ignorable", func(t *testing.T) {
		status = http.StatusBadRequest
		_, err := repo.GetAspectNode("urn:aspect:400")
		if !errors.Is(err, model.MessageIgnoreError) {
			t.Error(err)
		}
	})

	t.Run("keeps an internal service error retryable", func(t *testing.T) {
		status = http.StatusInternalServerError
		_, err := repo.GetAspectNode("urn:aspect:500")
		if err == nil {
			t.Error("expected an error")
			return
		}
		if errors.Is(err, model.MessageIgnoreError) {
			t.Error(err)
		}
	})

	t.Run("keeps a temporarily unavailable service retryable", func(t *testing.T) {
		status = http.StatusServiceUnavailable
		_, err := repo.GetAspectNode("urn:aspect:503")
		if err == nil {
			t.Error("expected an error")
			return
		}
		if errors.Is(err, model.MessageIgnoreError) {
			t.Error(err)
		}
	})
}

// TestDeviceRepoAspectNodeList covers the batched read the path selection uses: the aspects
// of one event description are read in a single request, and an id the device-repository
// does not know has to be reported rather than silently dropped from the answer.
func TestDeviceRepoAspectNodeList(t *testing.T) {
	known := map[string]models.AspectNode{
		"urn:aspect:1": {Id: "urn:aspect:1", Name: "one"},
		"urn:aspect:2": {Id: "urn:aspect:2", Name: "two"},
		"urn:aspect:3": {Id: "urn:aspect:3", Name: "three"},
	}
	requests := []string{}
	repo := newTestDeviceRepo(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if id, isSingle := strings.CutPrefix(request.URL.Path, "/aspect-nodes/"); isSingle {
			requests = append(requests, request.URL.Path)
			node, ok := known[id]
			if !ok {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			err := json.NewEncoder(writer).Encode(node)
			if err != nil {
				t.Error(err)
			}
			return
		}
		requests = append(requests, request.URL.Path+"?ids="+request.URL.Query().Get("ids"))
		nodes := []models.AspectNode{}
		for _, id := range strings.Split(request.URL.Query().Get("ids"), ",") {
			if node, ok := known[id]; ok {
				nodes = append(nodes, node)
			}
		}
		//the client of the device-repository reads the total of a list answer from the
		//header and fails without it
		writer.Header().Set("X-Total-Count", strconv.Itoa(len(nodes)))
		err := json.NewEncoder(writer).Encode(nodes)
		if err != nil {
			t.Error(err)
		}
	}))

	t.Run("reads several aspect-nodes in one request", func(t *testing.T) {
		nodes, err := repo.GetAspectNodes([]string{"urn:aspect:1", "urn:aspect:2"})
		if err != nil {
			t.Error(err)
			return
		}
		if len(nodes) != 2 {
			t.Error(nodes)
			return
		}
		if !reflect.DeepEqual(requests, []string{"/v2/aspect-nodes?ids=urn:aspect:1,urn:aspect:2"}) {
			t.Error(requests)
		}
	})

	t.Run("answers a repeated read from the cache", func(t *testing.T) {
		before := len(requests)
		_, err := repo.GetAspectNodes([]string{"urn:aspect:1", "urn:aspect:2"})
		if err != nil {
			t.Error(err)
			return
		}
		if len(requests) != before {
			t.Error(requests)
		}
	})

	t.Run("reads the same aspect id twice as one aspect", func(t *testing.T) {
		before := len(requests)
		nodes, err := repo.GetAspectNodes([]string{"urn:aspect:2", "urn:aspect:2"})
		if err != nil {
			t.Error(err)
			return
		}
		if len(nodes) != 1 {
			t.Error(nodes)
			return
		}
		if !reflect.DeepEqual(requests[before:], []string{"/v2/aspect-nodes?ids=urn:aspect:2"}) {
			t.Error(requests[before:])
		}
	})

	t.Run("caches a list and a single read of the same aspect side by side", func(t *testing.T) {
		//a one element list would share the cache key of the single aspect-node if it were
		//keyed by its ids alone. The cache recovers from that by refetching, so the cost is
		//not a wrong answer but a miss on every alternation between the two readers
		_, err := repo.GetAspectNodes([]string{"urn:aspect:3"})
		if err != nil {
			t.Error(err)
			return
		}
		_, err = repo.GetAspectNode("urn:aspect:3")
		if err != nil {
			t.Error(err)
			return
		}
		before := len(requests)
		_, err = repo.GetAspectNodes([]string{"urn:aspect:3"})
		if err != nil {
			t.Error(err)
			return
		}
		_, err = repo.GetAspectNode("urn:aspect:3")
		if err != nil {
			t.Error(err)
			return
		}
		if len(requests) != before {
			t.Error("expected both reads to be cached, got", requests[before:])
		}
	})

	t.Run("reports the aspect ids the device-repository does not know", func(t *testing.T) {
		_, err := repo.GetAspectNodes([]string{"urn:aspect:1", "urn:aspect:unknown"})
		if !errors.Is(err, model.MessageIgnoreError) {
			t.Error(err)
			return
		}
		if !strings.Contains(err.Error(), "urn:aspect:unknown") {
			t.Error(err)
		}
	})

	t.Run("reads nothing without an aspect id", func(t *testing.T) {
		before := len(requests)
		nodes, err := repo.GetAspectNodes(nil)
		if err != nil {
			t.Error(err)
			return
		}
		if len(nodes) != 0 {
			t.Error(nodes)
			return
		}
		if len(requests) != before {
			t.Error("expected no request to the device-repository", requests[before:])
		}
	})
}
