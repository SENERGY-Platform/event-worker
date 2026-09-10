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

package devicerepo

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/SENERGY-Platform/device-repository/v2/lib/client"
	devicerepomodel "github.com/SENERGY-Platform/device-repository/v2/lib/model"
	"github.com/SENERGY-Platform/event-worker/pkg/auth"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	marshallermodel "github.com/SENERGY-Platform/marshaller/lib/marshaller/model"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/service-commons/pkg/cache"
	"github.com/SENERGY-Platform/service-commons/pkg/cache/fallback"
	"github.com/SENERGY-Platform/service-commons/pkg/signal"
)

func New(ctx context.Context, wg *sync.WaitGroup, config configuration.Config, auth *auth.Auth) (result *DeviceRepo, err error) {
	cacheDuration, err := time.ParseDuration(config.DeviceRepoCacheDuration)
	if err != nil {
		return result, err
	}
	result = &DeviceRepo{
		auth:          auth,
		config:        config,
		cacheDuration: cacheDuration,
	}
	result.client = client.NewClient(config.DeviceRepoUrl, result.getToken)

	cacheConfig := cache.Config{
		CacheInvalidationSignalHooks: map[cache.Signal]cache.ToKey{
			signal.Known.CacheInvalidationAll: nil,
			signal.Known.ConceptCacheInvalidation: func(signalValue string) (cacheKey string) {
				return "concept." + signalValue
			},
			signal.Known.CharacteristicCacheInvalidation: func(signalValue string) (cacheKey string) {
				return "characteristics." + signalValue
			},
			signal.Known.FunctionCacheInvalidation: func(signalValue string) (cacheKey string) {
				return "functions." + signalValue
			},
			signal.Known.AspectCacheInvalidation: nil, //invalidate everything, because an aspect corresponds to multiple aspect-nodes
		},
	}
	if config.Mode == configuration.FogMode && config.FallbackFile != "" && config.FallbackFile != "-" {
		cacheConfig.FallbackProvider = fallback.NewProvider(config.FallbackFile)
	}
	result.cache, err = cache.New(cacheConfig)
	if err != nil {
		return result, err
	}
	return result, nil
}

type DeviceRepo struct {
	auth          *auth.Auth
	cache         *cache.Cache
	client        client.Interface
	config        configuration.Config
	cacheDuration time.Duration
}

// clientError keeps the retry decision the hand written requests made before the
// device-repository client replaced them: an internal service error may be retried,
// every other answer is final for this message and is marked as ignorable, so that the
// worker drops the message instead of retrying it forever. The client reports transport
// and decoding failures as http.StatusInternalServerError, so those are retried as well.
func clientError(err error, code int) error {
	if err == nil {
		return nil
	}
	if code >= http.StatusInternalServerError {
		return err
	}
	return fmt.Errorf("%w: %v", model.MessageIgnoreError, err.Error())
}

func (this *DeviceRepo) getToken() (string, error) {
	if this.auth == nil {
		this.auth = &auth.Auth{}
	}
	return this.auth.EnsureAccess(this.config)
}

func (this *DeviceRepo) GetCharacteristic(id string) (result models.Characteristic, err error) {
	use := cache.Use[models.Characteristic]
	if this.config.AsyncCacheRefresh {
		use = cache.UseWithAsyncRefresh[models.Characteristic]
	}
	return use(this.cache, "characteristics."+id, func() (result models.Characteristic, err error) {
		return this.getCharacteristic(id)
	}, func(characteristic models.Characteristic) error {
		if characteristic.Id == "" {
			return errors.New("invalid characteristic returned from cache")
		}
		return nil
	}, this.cacheDuration)
}

func (this *DeviceRepo) getCharacteristic(id string) (result models.Characteristic, err error) {
	result, err, code := this.client.GetCharacteristic(id)
	return result, clientError(err, code)
}

func (this *DeviceRepo) GetConcept(id string) (result models.Concept, err error) {
	use := cache.Use[models.Concept]
	if this.config.AsyncCacheRefresh {
		use = cache.UseWithAsyncRefresh[models.Concept]
	}
	return use(this.cache, "concept."+id, func() (result models.Concept, err error) {
		return this.getConcept(id)
	}, func(concept models.Concept) error {
		if concept.Id == "" {
			return errors.New("invalid concept returned from cache")
		}
		return nil
	}, this.cacheDuration)
}

func (this *DeviceRepo) getConcept(id string) (result models.Concept, err error) {
	//the marshaller needs the characteristic ids and the conversions of the concept, not
	//the characteristics themselves, which is what the device-repository answers by default
	result, err, code := this.client.GetConceptWithoutCharacteristics(id)
	return result, clientError(err, code)
}

func (this *DeviceRepo) GetConceptIdOfFunction(id string) string {
	function, err := this.GetFunction(id)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		return ""
	}
	return function.ConceptId
}

func (this *DeviceRepo) GetFunction(id string) (result models.Function, err error) {
	use := cache.Use[models.Function]
	if this.config.AsyncCacheRefresh {
		use = cache.UseWithAsyncRefresh[models.Function]
	}
	return use(this.cache, "functions."+id, func() (result models.Function, err error) {
		return this.getFunction(id)
	}, func(function models.Function) error {
		if function.Id == "" {
			return errors.New("invalid function returned from cache")
		}
		return nil
	}, this.cacheDuration)
}

func (this *DeviceRepo) getFunction(id string) (result models.Function, err error) {
	result, err, code := this.client.GetFunction(id)
	return result, clientError(err, code)
}

func (this *DeviceRepo) GetAspectNode(id string) (result models.AspectNode, err error) {
	use := cache.Use[models.AspectNode]
	if this.config.AsyncCacheRefresh {
		use = cache.UseWithAsyncRefresh[models.AspectNode]
	}
	return use(this.cache, "aspect-nodes."+id, func() (result models.AspectNode, err error) {
		return this.getAspectNode(id)
	}, func(node models.AspectNode) error {
		if node.Id == "" {
			return errors.New("invalid aspect-node returned from cache")
		}
		return nil
	}, this.cacheDuration)
}

func (this *DeviceRepo) getAspectNode(id string) (result models.AspectNode, err error) {
	result, err, code := this.client.GetAspectNode(id)
	return result, clientError(err, code)
}

// GetAspectNodes reads the aspect-nodes of several ids in one request instead of one
// request per id. Every requested id has to resolve, because a criteria naming an aspect
// that does not exist is unanswerable: a missing id is reported the way the not found of a
// single read is, so that the worker drops the message instead of retrying it.
//
// The answer is ordered by the device-repository and not by the requested ids, which the
// aspect matching does not depend on: it takes the worst matched aspect of the whole set.
func (this *DeviceRepo) GetAspectNodes(ids []string) (result []models.AspectNode, err error) {
	//reading the same id twice is one read, and the deduplicated count is what lets the
	//length of the answer stand for 'every requested id resolved'
	ids = slices.Clone(ids)
	slices.Sort(ids)
	ids = slices.Compact(ids)
	if len(ids) == 0 {
		return []models.AspectNode{}, nil
	}
	use := cache.Use[[]models.AspectNode]
	if this.config.AsyncCacheRefresh {
		use = cache.UseWithAsyncRefresh[[]models.AspectNode]
	}
	//a key of its own, because a single aspect-node is cached under its id alone and would
	//collide with a one element list
	return use(this.cache, "aspect-node-list."+strings.Join(ids, ","), func() (result []models.AspectNode, err error) {
		return this.getAspectNodes(ids)
	}, func(nodes []models.AspectNode) error {
		if len(nodes) != len(ids) {
			return errors.New("invalid aspect-nodes returned from cache")
		}
		return nil
	}, this.cacheDuration)
}

func (this *DeviceRepo) getAspectNodes(ids []string) (result []models.AspectNode, err error) {
	result, _, err, code := this.client.ListAspectNodes(devicerepomodel.AspectListOptions{Ids: ids})
	if err != nil {
		return result, clientError(err, code)
	}
	if len(result) != len(ids) {
		return result, fmt.Errorf("%w: unknown aspect nodes: %v", model.MessageIgnoreError, strings.Join(missingAspectNodes(ids, result), ", "))
	}
	return result, nil
}

func missingAspectNodes(ids []string, nodes []models.AspectNode) (result []string) {
	for _, id := range ids {
		if !marshallermodel.ContainsAspectNode(nodes, id) {
			result = append(result, id)
		}
	}
	return result
}
