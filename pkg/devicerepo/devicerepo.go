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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/SENERGY-Platform/event-worker/pkg/auth"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
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
	config        configuration.Config
	cacheDuration time.Duration
}

func (this *DeviceRepo) GetJson(token string, endpoint string, result interface{}) (err error) {
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", token)
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		//internal service errors may be retried
		temp, _ := io.ReadAll(resp.Body)
		return errors.New(strings.TrimSpace(string(temp)))
	}
	if resp.StatusCode >= 300 {
		temp, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: %v", model.MessageIgnoreError, strings.TrimSpace(string(temp)))
	}
	err = json.NewDecoder(resp.Body).Decode(result)
	if err != nil {
		log.Println("ERROR:", err.Error())
		debug.PrintStack()
		return fmt.Errorf("%w: %v", model.MessageIgnoreError, err.Error())
	}
	return nil
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
	token, err := this.getToken()
	if err != nil {
		return result, err
	}
	err = this.GetJson(token, this.config.DeviceRepoUrl+"/characteristics/"+url.PathEscape(id), &result)
	return
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
	token, err := this.getToken()
	if err != nil {
		return result, err
	}
	err = this.GetJson(token, this.config.DeviceRepoUrl+"/concepts/"+url.PathEscape(id), &result)
	return
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
	token, err := this.getToken()
	if err != nil {
		return result, err
	}
	err = this.GetJson(token, this.config.DeviceRepoUrl+"/functions/"+url.PathEscape(id), &result)
	return
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
	token, err := this.getToken()
	if err != nil {
		return result, err
	}
	err = this.GetJson(token, this.config.DeviceRepoUrl+"/aspect-nodes/"+url.QueryEscape(id), &result)
	return
}
